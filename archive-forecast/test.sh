#!/bin/bash
# Copyright (c) 2026 INFISYS INC

set -e

failed=0

pushd archive_forecast
build/python/bin/python -m pytest --cov --cov-report=term-missing || failed=1
popd

pushd graphcast_gfs
build/python/bin/python -m pytest --cov --cov-report=term-missing || failed=1
popd

pushd weathernext_ifs
build/python/bin/python -m pytest --cov --cov-report=term-missing || failed=1
popd

pushd weathernext_3
build/python/bin/python -m pytest --cov --cov-report=term-missing || failed=1
popd

if [ "$failed" -ne 0 ]; then
	echo ERROR
	exit 1
fi

echo SUCCESS
