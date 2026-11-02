#!/bin/bash

set -xeo pipefail

TODAY="$(date +%Y-%m-%d)"
MY_SECRET="$(cat .password)"

cd "$(dirname $0)"
time gradle run --args="two-months.json"

cd output
zip --password "${MY_SECRET}" -r "zips/${TODAY}.zip" "${TODAY}"
