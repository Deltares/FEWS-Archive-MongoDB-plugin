#!/bin/bash
# Copyright (c) 2026 INFISYS INC

# [config_path]
# ./run.sh "/root/_git/FEWS-Archive/archive-forecast/weathernext_3/config/posix.xml"

set -e

d=$( dirname "$0" )

if [ ! -f "$d/.installed" ]; then
    "$d/python/bin/python" -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: setuptools wheel
    "$d/python/bin/python" -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: -r "$d/requirements.txt"
    touch "$d/.installed"
fi

"$d/python/bin/python" -m weathernext_3 --config_path "$1"
