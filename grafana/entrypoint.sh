#!/bin/sh
set -eu

# Grafana infers numeric types when expanding jsonData environment variables,
# even when the YAML value is quoted. Render concrete JSON strings first.
# JSON is valid YAML. Escape dollars for Grafana's subsequent interpolation.
source_dir="${GF_PATHS_PROVISIONING:-/etc/grafana/provisioning}"
runtime_dir="$(mktemp -d /tmp/perps-provisioning.XXXXXX)"
cp -R "$source_dir/." "$runtime_dir/"
jq --arg user "${DDB_USER:-admin}" --arg password "${DDB_PASSWORD:-123456}" '
    .datasources[0].jsonData.username = ($user | gsub("\\$"; "$$")) |
    .datasources[0].jsonData.password = ($password | gsub("\\$"; "$$"))
' "$source_dir/datasources/dolphindb.yaml" > "$runtime_dir/datasources/dolphindb.yaml"
export GF_PATHS_PROVISIONING="$runtime_dir"
exec /run.sh "$@"
