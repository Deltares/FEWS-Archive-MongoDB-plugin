#!/bin/bash
# Copyright (c) 2026 INFISYS INC

set -e

rm -rf build
mkdir -p build

tar -xzf ../install/cpython*.tar.gz -C build

build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --upgrade pip
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: setuptools wheel
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: -r requirements.txt

rm -rf src/archive_forecast.egg-info
rm -rf build/bdist.linux-x86_64
rm -rf build/lib
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --only-binary=:all: --group dev
build/python/bin/python -m pytest --cov --cov-report=term-missing

echo SUCCESS
