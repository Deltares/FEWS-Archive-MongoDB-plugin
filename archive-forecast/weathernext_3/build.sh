#!/bin/bash
# Copyright (c) 2026 INFISYS INC

set -e

rm -rf build
mkdir -p build

tar -xzf ../install/cpython*.tar.gz -C build

build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --upgrade pip
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: setuptools
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: -r requirements.txt
build/python/bin/python -m pip freeze > build/requirements.txt
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation ../archive_forecast
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation .

\cp -f run.sh build/
\cp -f weathernext3tva.json build/

rm -rf src/weathernext_3.egg-info
rm -rf build/bdist.linux-x86_64
rm -rf build/lib
rm -rf ../archive_forecast/src/archive_forecast.egg-info
rm -rf ../archive_forecast/build/bdist.linux-x86_64
rm -rf ../archive_forecast/build/lib

rm -f weathernext_3.zip
cd build && 7z a -r -snl ../weathernext_3.zip && cd ..

build/python/bin/python -m pip uninstall --root-user-action ignore -y -r build/requirements.txt
rm -f weathernext_3_min.zip
cd build && 7z a -r -snl ../weathernext_3_min.zip && cd ..

build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: setuptools
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: -r build/requirements.txt
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --no-deps -e ../archive_forecast
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --no-deps -e .
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: --group dev

rm -rf src/weathernext_3.egg-info
rm -rf ../archive_forecast/src/archive_forecast.egg-info

build/python/bin/python -m pytest --cov --cov-report=term-missing

echo SUCCESS
