#!/bin/bash
# Copyright (c) 2025 INFISYS INC

# [config_path]
# ./weathernext_ifs.sh "/root/_git/FEWS-Archive/archive-forecast/configurations/ifs_posix_config.xml"

set -e

d=$( dirname "$0" )

export XLA_PYTHON_CLIENT_PREALLOCATE=false
export XLA_PYTHON_CLIENT_ALLOCATOR=platform
export CDSAPI_RC=$d/GraphCastOperationalIfs/.cdsapirc

"$d/python/bin/python" -m archive_forecast --provider weathernext_ifs --assets_path "$d/GraphCastOperationalIfs" --config_path "$1"
