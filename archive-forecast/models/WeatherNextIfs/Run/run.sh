#!/bin/bash
# Copyright (c) 2026 INFISYS INC

# [config_path]
# ./run.sh "/root/_git/FEWS-Archive/archive-forecast/configurations/config_posix.xml"

set -e

d=$( dirname "$0" )

if [ ! -f "$d/.installed" ]; then
    "$d/python/bin/python" -m pip install --root-user-action ignore --no-build-isolation --only-binary=:all: setuptools wheel
    "$d/python/bin/python" -m pip install --root-user-action ignore --no-build-isolation --only-binary=:all: -r "$d/requirements.txt"
    touch "$d/.installed"
fi

export XLA_PYTHON_CLIENT_PREALLOCATE=false
export XLA_PYTHON_CLIENT_ALLOCATOR=platform
export CDSAPI_RC=$d/.cdsapirc

"$d/python/bin/python" -m archive_forecast --provider weathernext_ifs --assets_path "$d/WeatherNextIfs" --config_path "$1"
