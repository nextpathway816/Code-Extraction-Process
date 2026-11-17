#!/usr/bin/env bash
# Teradata DDL extract (Next Pathway)
# Edit only the two lines below. See the guide, Step 1.
include_databases="LIKE('%')"
exclude_databases="NOT IN('DBC','TD_SYSFNLIB','SYSLIB','SYSUDTLIB','SQLJ','SYSDBA')"

# ---- Do not edit below this line ----
set -u
cd "$(dirname "$0")" || exit 1
OUT="output"

if ! command -v bteq >/dev/null 2>&1; then
  echo "bteq: command not found. Use a server with Teradata Tools and Utilities installed."
  exit 1
fi
if [ -d "$OUT" ] && [ -n "$(ls -A "$OUT" 2>/dev/null)" ]; then
  echo "The '$OUT' folder already has files. Move them to another folder, then run again."
  exit 1
fi
mkdir -p "$OUT"

# Build a temporary run file with the scope values filled in.
# create_ddls.btq itself is never changed. The temporary file is deleted on exit.
RUN="$(mktemp)"
chmod 600 "$RUN"
trap 'rm -f "$RUN"' EXIT
esc() { printf '%s' "$1" | sed -e 's/[|&\\]/\\&/g'; }
sed -e "s|<<INCLUDE>>|$(esc "$include_databases")|g" \
    -e "s|<<EXCLUDE>>|$(esc "$exclude_databases")|g" \
    -e "s|<<OUT>>|$OUT|g" create_ddls.btq > "$RUN"

echo "Running the extract. This can take a while on large systems..."
bteq < "$RUN" > "$OUT/create_ddls.log" 2>&1
rc=$?
echo "Finished (return code $rc). Check $OUT/create_ddls.log for '*** Failure'."
exit $rc
