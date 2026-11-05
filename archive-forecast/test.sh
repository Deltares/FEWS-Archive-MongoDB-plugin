#!/bin/bash
# Copyright (c) 2026 INFISYS INC

pushd archive_forecast
build/python/bin/python -m pytest
popd

pushd graphcast_gfs
build/python/bin/python -m pytest
popd

pushd weathernext_ifs
build/python/bin/python -m pytest
popd

pushd weathernext_3
build/python/bin/python -m pytest
popd
