#!/bin/bash
# Copyright (c) 2026 INFISYS INC

# [config_path] [weights_path]
# ./run.sh "/root/_git/FEWS-Archive/archive-forecast/graphcast_gfs/config/posix.xml" "/root/_git/FEWS-Archive/archive-forecast/graphcast_gfs/weights"

set -e

d=$( dirname "$0" )

if [ ! -f "$d/.installed" ]; then
    "$d/python/bin/python" -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: setuptools wheel
    "$d/python/bin/python" -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: -r "$d/requirements.txt"
    touch "$d/.installed"
fi

export XLA_PYTHON_CLIENT_PREALLOCATE=false
export XLA_PYTHON_CLIENT_ALLOCATOR=platform

"$d/python/bin/python" -m graphcast_gfs --config_path "$1" --weights_path "$2"
