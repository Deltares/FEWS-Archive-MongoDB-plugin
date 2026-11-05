#!/bin/bash
# Copyright (c) 2026 INFISYS INC

set -e

rm -rf build
mkdir -p build

tar -xzf ../install/cpython*.tar.gz -C build

build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --upgrade pip
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: setuptools wheel
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: -r requirements.txt
build/python/bin/python -m pip freeze > build/requirements.txt
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: model/model.zip
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation ../archive_forecast
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation .

\cp -f run.sh build/

rm -rf src/weathernext_ifs.egg-info
rm -rf build/bdist.linux-x86_64
rm -rf build/lib
rm -rf ../archive_forecast/src/archive_forecast.egg-info
rm -rf ../archive_forecast/build/bdist.linux-x86_64
rm -rf ../archive_forecast/build/lib

rm -f weathernext_ifs_weights.zip
cd weights && 7z a -r -snl ../weathernext_ifs_weights.zip && cd ..

rm -f weathernext_ifs.zip
cd build && 7z a -r -snl ../weathernext_ifs.zip && cd ..

build/python/bin/python -m pip uninstall --root-user-action ignore -y -r build/requirements.txt
rm -f weathernext_ifs_min.zip
cd build && 7z a -r -snl ../weathernext_ifs_min.zip && cd ..

build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: setuptools wheel
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: -r build/requirements.txt
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --no-deps -e ../archive_forecast
rm -rf ../archive_forecast/src/archive_forecast.egg-info
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --no-deps -e .
rm -rf src/weathernext_ifs.egg-info
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --only-binary=:all: --group dev

echo SUCCESS
