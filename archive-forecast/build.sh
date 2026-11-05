#!/bin/bash
# Copyright (c) 2026 INFISYS INC

set -e

pushd archive_forecast
bash build.sh
popd

pushd graphcast_gfs
bash build.sh
popd

pushd weathernext_ifs
bash build.sh
popd

pushd weathernext_3
bash build.sh
popd
