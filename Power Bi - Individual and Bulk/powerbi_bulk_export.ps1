<#
.SYNOPSIS
    Exports Power BI reports from the workspaces you can access, using the Power BI REST API.

.DESCRIPTION
    Signs in to the Power BI service, lists the workspaces you are a member of (or the ones you name),
    lists every report in them and downloads each one:
      - Power BI reports as .pbix
      - Paginated reports as .rdl
    Each workspace gets its own folder. Every report is recorded in export_manifest.csv, written as
    the export runs, so a stopped run still leaves a record. Run it again with -SkipExisting to resume.

    Requires the MicrosoftPowerBIMgmt PowerShell module and at least the Contributor role on each workspace.

.PARAMETER OutputFolder
    Folder for the export. Created if it does not exist.

.PARAMETER Workspace
    Optional. One or more workspace names to export. If omitted, every workspace you are a member of is exported.

.PARAMETER DownloadType
    IncludeModel (default) downloads the report with its semantic model and data.
    LiveConnect downloads the report with a live connection to the semantic model and no data.

.PARAMETER NoLiveConnectFallback
    By default, if an IncludeModel download fails, the script retries the report with LiveConnect.
    Use this switch to turn that off.

.PARAMETER SkipExisting
    Skip reports whose file already exists in the output folder and passes the file checks.

.PARAMETER ListOnly
    Sign in and list the workspaces and reports that would be exported, without downloading anything.
    Writes export_manifest.csv with status 'listed'. Use this as a safe first run.

.PARAMETER TimeoutSec
    Time allowed for each download, in seconds. Default 3600.

.EXAMPLE
    .\powerbi_bulk_export.ps1 -OutputFolder .\powerbi_export

.EXAMPLE
    .\powerbi_bulk_export.ps1 -OutputFolder .\powerbi_export -Workspace 'Finance', 'Sales'
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$OutputFolder,
    [string[]]$Workspace,
    [ValidateSet('IncludeModel', 'LiveConnect')][string]$DownloadType = 'IncludeModel',
    [switch]$NoLiveConnectFallback,
    [switch]$SkipExisting,
    [switch]$ListOnly,
    [int]$TimeoutSec = 3600
)

$ErrorActionPreference = 'Stop'

if (-not (Get-Module -ListAvailable -Name MicrosoftPowerBIMgmt.Profile)) {
    Write-Error 'MicrosoftPowerBIMgmt module not found. Run: Install-Module -Name MicrosoftPowerBIMgmt -Scope CurrentUser'
    exit 1
}
Import-Module MicrosoftPowerBIMgmt.Profile
Import-Module MicrosoftPowerBIMgmt.Workspaces
Add-Type -AssemblyName System.IO.Compression.FileSystem

function Get-SafeName {
    param([string]$Value)
    if (-not $Value) { return 'unnamed' }
    $invalid = [System.IO.Path]::GetInvalidFileNameChars()
    $chars = foreach ($c in $Value.ToCharArray()) { if ($invalid -contains $c) { '_' } else { [string]$c } }
    $name = (-join $chars).Trim(' ', '.')
    if ($name.Length -gt 80) { $name = $name.Substring(0, 80).Trim(' ', '.') }
    if (-not $name) { $name = 'unnamed' }
    return $name
}

function Get-ShortId {
    param([string]$Id)
    if ($Id.Length -ge 8) { return $Id.Substring(0, 8) }
    return $Id
}

function Test-ExportFile {
    param([string]$Path, [string]$Extension)
    $item = Get-Item -LiteralPath $Path
    if ($item.Length -eq 0) { throw 'Downloaded file is empty' }
    if ($Extension -eq '.pbix') {
        try {
            $zip = [System.IO.Compression.ZipFile]::OpenRead($item.FullName)
            $zip.Dispose()
        }
        catch { throw 'Downloaded .pbix is not a valid package' }
    }
    else {
        try { [xml](Get-Content -LiteralPath $item.FullName -Raw) | Out-Null }
        catch { throw 'Downloaded .rdl is not valid XML' }
    }
}

function Save-Report {
    param([string]$WorkspaceId, [string]$ReportId, [string]$Mode, [string]$Target, [string]$Extension)
    $query = @('preferClientRouting=true')
    if ($Mode) { $query += "downloadType=$Mode" }
    $url = "groups/$WorkspaceId/reports/$ReportId/Export?" + ($query -join '&')
    $temp = "$Target.part"
    try {
        if (Test-Path -LiteralPath $temp) { Remove-Item -LiteralPath $temp -Force }
        Invoke-PowerBIRestMethod -Method Get -Url $url -OutFile $temp -TimeoutSec $TimeoutSec -ErrorAction Stop | Out-Null
        if (-not (Test-Path -LiteralPath $temp)) { throw 'No file was written' }
        Test-ExportFile -Path $temp -Extension $Extension
        if (Test-Path -LiteralPath $Target) { Remove-Item -LiteralPath $Target -Force }
        Move-Item -LiteralPath $temp -Destination $Target
    }
    finally {
        if (Test-Path -LiteralPath $temp) { Remove-Item -LiteralPath $temp -Force }
    }
}

function Write-ManifestRow {
    param($Row)
    [pscustomobject]$Row | Export-Csv -LiteralPath $script:Manifest -NoTypeInformation -Append -Encoding UTF8
}

New-Item -ItemType Directory -Path $OutputFolder -Force | Out-Null
$root = (Resolve-Path -LiteralPath $OutputFolder).ProviderPath
$script:Manifest = Join-Path $root 'export_manifest.csv'
if (Test-Path -LiteralPath $script:Manifest) {
    $stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
    Rename-Item -LiteralPath $script:Manifest -NewName "export_manifest_$stamp.csv"
}

$total = 0
$failed = 0
$fatal = $false

try {
    Write-Host 'Signing in to Power BI. Complete the sign-in window that opens.'
    Connect-PowerBIServiceAccount | Out-Null

    $workspaces = @(Get-PowerBIWorkspace -Scope Individual -All)
    if ($Workspace) {
        $requested = $Workspace
        $workspaces = @($workspaces | Where-Object { $requested -contains $_.Name })
        foreach ($name in $requested) {
            if (-not ($workspaces | Where-Object { $_.Name -eq $name })) {
                Write-Warning "Workspace not found or not accessible: $name"
            }
        }
    }
    Write-Host "Found $($workspaces.Count) workspace(s) to export."

    foreach ($ws in $workspaces) {
        $wsId = [string]$ws.Id
        $response = Invoke-PowerBIRestMethod -Method Get -Url "groups/$wsId/reports" -ErrorAction Stop | ConvertFrom-Json
        $reports = @($response.value)
        Write-Host ''
        Write-Host "Workspace '$($ws.Name)': $($reports.Count) report(s)."
        if ($reports.Count -eq 0) { continue }

        $folder = Join-Path $root ('{0}__{1}' -f (Get-SafeName $ws.Name), (Get-ShortId $wsId))
        if (-not $ListOnly) { New-Item -ItemType Directory -Path $folder -Force | Out-Null }

        foreach ($report in $reports) {
            $total++
            $reportType = 'PowerBIReport'
            if ($report.PSObject.Properties['reportType'] -and $report.reportType) { $reportType = [string]$report.reportType }
            $isPaginated = ($reportType -eq 'PaginatedReport')
            $extension = '.pbix'
            if ($isPaginated) { $extension = '.rdl' }
            $datasetId = ''
            if ($report.PSObject.Properties['datasetId'] -and $report.datasetId) { $datasetId = [string]$report.datasetId }

            $target = Join-Path $folder ('{0}__{1}{2}' -f (Get-SafeName $report.name), (Get-ShortId ([string]$report.id)), $extension)
            $row = [ordered]@{
                workspace    = $ws.Name
                workspace_id = $wsId
                report       = $report.name
                report_id    = [string]$report.id
                report_type  = $reportType
                dataset_id   = $datasetId
                mode         = ''
                file         = ''
                bytes        = ''
                status       = ''
                error        = ''
            }

            if ($ListOnly) {
                $row.status = 'listed'
                Write-ManifestRow $row
                Write-Host "LIST $($report.name) [$reportType]"
                continue
            }

            if ($SkipExisting -and (Test-Path -LiteralPath $target)) {
                $existingOk = $true
                try { Test-ExportFile -Path $target -Extension $extension } catch { $existingOk = $false }
                if ($existingOk) {
                    $row.mode = 'existing'
                    $row.file = $target.Substring($root.Length).TrimStart('\', '/')
                    $row.bytes = (Get-Item -LiteralPath $target).Length
                    $row.status = 'skipped_existing'
                    Write-ManifestRow $row
                    Write-Host "SKIP $($report.name) (already exported)"
                    continue
                }
            }

            if ($isPaginated) { $modes = @('') }
            else {
                $modes = @($DownloadType)
                if ($DownloadType -eq 'IncludeModel' -and -not $NoLiveConnectFallback) { $modes += 'LiveConnect' }
            }

            $errors = @()
            $done = $false
            foreach ($mode in $modes) {
                for ($attempt = 1; $attempt -le 2 -and -not $done; $attempt++) {
                    try {
                        Save-Report -WorkspaceId $wsId -ReportId ([string]$report.id) -Mode $mode -Target $target -Extension $extension
                        $done = $true
                        $row.mode = $mode
                        if ($isPaginated) { $row.mode = 'paginated' }
                    }
                    catch {
                        $label = $mode
                        if (-not $label) { $label = 'paginated' }
                        $errors += "${label} attempt ${attempt}: $($_.Exception.Message)"
                        if ($attempt -lt 2) { Start-Sleep -Seconds 10 }
                    }
                }
                if ($done) { break }
            }

            if ($done) {
                $row.file = $target.Substring($root.Length).TrimStart('\', '/')
                $row.bytes = (Get-Item -LiteralPath $target).Length
                $row.status = 'ok'
                if ($errors.Count -gt 0) { $row.error = ($errors -join ' | ') }
                Write-Host "OK   [$($row.mode)] $($report.name)"
            }
            else {
                $failed++
                $row.status = 'failed'
                $row.error = ($errors -join ' | ')
                Write-Host "FAIL $($report.name): $($errors[-1])" -ForegroundColor Red
            }
            Write-ManifestRow $row
        }
    }
}
catch {
    $fatal = $true
    Write-Host "Export stopped: $($_.Exception.Message)" -ForegroundColor Red
}
finally {
    try { Disconnect-PowerBIServiceAccount | Out-Null } catch { }
}

Write-Host ''
if ($ListOnly) {
    Write-Host "Listed: $total report(s). Nothing was downloaded. See export_manifest.csv."
}
else {
    Write-Host "Completed: $($total - $failed) exported or already present, $failed failed. See export_manifest.csv."
}
if ($fatal -or $failed -gt 0) { exit 1 }
exit 0
