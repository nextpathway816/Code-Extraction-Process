#!/usr/bin/env python3
"""Export Tableau workbooks from one site with the Tableau REST API."""
import argparse
import csv
import getpass
import re
import sys
import urllib.parse
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

NS = {"t": "http://tableau.com/api"}
FIELDS = ["site", "project", "project_id", "workbook", "workbook_id", "file", "bytes", "status", "error"]


def safe_name(value):
    value = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", value).strip(" .")
    return value[:100] or "unnamed"


def xml_root(response):
    response.raise_for_status()
    return ET.fromstring(response.content)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True, help="Base URL, e.g. https://prod-ca-a.online.tableau.com")
    parser.add_argument("--site", required=True, help="Site content URL from the Tableau browser URL")
    parser.add_argument("--pat-name", required=True, help="Personal access token name")
    parser.add_argument("--api-version", required=True, help="REST API version supported by your Tableau server, e.g. 3.29")
    parser.add_argument("--output", required=True, type=Path, help="Destination directory")
    parser.add_argument("--include-extracts", action="store_true", help="Include workbook extracts when Tableau provides them")
    args = parser.parse_args()
    if not re.fullmatch(r"\d+\.\d+", args.api_version):
        parser.error("--api-version must look like 3.29")
    server = args.server.rstrip("/")
    parsed = urllib.parse.urlparse(server)
    if parsed.scheme != "https" or not parsed.netloc or parsed.path:
        parser.error("--server must be an HTTPS base URL with no path")
    secret = getpass.getpass("Tableau PAT secret: ")
    if not secret:
        parser.error("PAT secret is required")
    base = f"{server}/api/{args.api_version}"
    session = requests.Session()
    session.headers.update({"Accept": "application/xml"})
    credentials = ET.Element("credentials", {"personalAccessTokenName": args.pat_name,
                                             "personalAccessTokenSecret": secret})
    ET.SubElement(credentials, "site", {"contentUrl": args.site})
    request_xml = ET.Element("tsRequest")
    request_xml.append(credentials)
    token = None
    rows = []
    args.output.mkdir(parents=True, exist_ok=True)
    try:
        root = xml_root(session.post(f"{base}/auth/signin", data=ET.tostring(request_xml),
                                     headers={"Content-Type": "application/xml"}, timeout=60))
        cred = root.find("t:credentials", NS)
        site = cred.find("t:site", NS) if cred is not None else None
        if cred is None or site is None:
            raise RuntimeError("Sign-in response did not contain credentials and site ID")
        token, site_id = cred.attrib["token"], site.attrib["id"]
        session.headers["X-Tableau-Auth"] = token
        workbooks = []
        page = 1
        while True:
            root = xml_root(session.get(f"{base}/sites/{site_id}/workbooks",
                                        params={"pageSize": 1000, "pageNumber": page, "sort": "name:asc"},
                                        timeout=60))
            paging = root.find("t:pagination", NS)
            if paging is None:
                raise RuntimeError(f"Page {page} had no pagination element")
            total = int(paging.attrib["totalAvailable"])
            batch = root.findall("t:workbooks/t:workbook", NS)
            workbooks.extend(batch)
            if len(workbooks) >= total:
                break
            if not batch:
                raise RuntimeError(f"Page {page} was empty before all {total} workbooks were retrieved")
            page += 1
        unique = {w.attrib["id"]: w for w in workbooks}
        if len(unique) != total:
            raise RuntimeError(f"Listed {len(unique)} unique workbooks; API says {total}. Check paging and rerun")
        print(f"Found {total} visible workbooks on site {args.site}.")
        for workbook_id, wb in unique.items():
            name = wb.attrib.get("name", "unnamed")
            project = wb.find("t:project", NS)
            project_name = project.attrib.get("name", "unknown_project") if project is not None else "unknown_project"
            project_id = project.attrib.get("id", "") if project is not None else ""
            row = {"site": args.site, "project": project_name, "project_id": project_id,
                   "workbook": name, "workbook_id": workbook_id, "file": "", "bytes": "", "status": "", "error": ""}
            try:
                url = f"{base}/sites/{site_id}/workbooks/{workbook_id}/content"
                params = None if args.include_extracts else {"includeExtract": "False"}
                with session.get(url, params=params, headers={"Accept": "*/*"}, stream=True, timeout=(30, 300)) as response:
                    response.raise_for_status()
                    disposition = response.headers.get("Content-Disposition", "")
                    match = re.search(r'filename\s*=\s*"?([^";]+)', disposition, re.I)
                    supplied = match.group(1).strip() if match else ""
                    ext = Path(supplied).suffix.lower()
                    if ext not in (".twb", ".twbx"):
                        raise RuntimeError("Download response did not identify a .twb or .twbx file")
                    folder = args.output / f"{safe_name(project_name)}__{project_id[:8]}"
                    folder.mkdir(parents=True, exist_ok=True)
                    filename = f"{safe_name(name)}__{workbook_id[:8]}{ext}"
                    path = folder / filename
                    temp = path.with_suffix(path.suffix + ".part")
                    try:
                        with temp.open("wb") as output:
                            for chunk in response.iter_content(chunk_size=1024 * 1024):
                                if chunk:
                                    output.write(chunk)
                        if temp.stat().st_size == 0:
                            raise RuntimeError("Downloaded file is empty")
                        if ext == ".twbx" and not zipfile.is_zipfile(temp):
                            raise RuntimeError("Downloaded .twbx is not a valid ZIP package")
                        if ext == ".twb":
                            ET.parse(temp)
                        temp.replace(path)
                    finally:
                        temp.unlink(missing_ok=True)
                row.update(file=str(path.relative_to(args.output)), bytes=path.stat().st_size, status="ok")
                print(f"OK  {name} -> {row['file']}")
            except (requests.RequestException, OSError, RuntimeError, ET.ParseError) as exc:
                row.update(status="failed", error=str(exc))
                print(f"FAIL {name}: {exc}", file=sys.stderr)
            rows.append(row)
    finally:
        if token:
            try:
                session.post(f"{base}/auth/signout", timeout=30)
            except requests.RequestException:
                pass
        session.close()
        with (args.output / "export_manifest.csv").open("w", newline="", encoding="utf-8") as output:
            writer = csv.DictWriter(output, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows(rows)
    failures = sum(row["status"] != "ok" for row in rows)
    print(f"Completed: {len(rows) - failures} downloaded, {failures} failed. See export_manifest.csv.")
    return 1 if failures else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (requests.RequestException, ET.ParseError, RuntimeError, KeyError, ValueError) as exc:
        print(f"Export stopped: {exc}", file=sys.stderr)
        sys.exit(1)
