#!/bin/bash

set -xeo pipefail

TODAY="$(date +%Y-%m-%d)"
MY_SECRET="$(cat .password)"

cd "$(dirname $0)"
time gradle run --args="six-months.json"
#time gradle run --args="two-months-makeup.json"

cd output
zip --password "${MY_SECRET}" -r "zips/${TODAY}.zip" "${TODAY}"

open "https://sonj.sharepoint.com/:f:/s/DOLUIModernization363/IgBsc5KsSa7TSZfuBMg25_DBARFbwwv0jLITZiWvTc1jrbY?e=R3rc2k"
