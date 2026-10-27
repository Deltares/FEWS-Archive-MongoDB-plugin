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
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation .

\cp -f run.sh build/

rm -rf weathernext_3.egg-info
rm -rf build/bdist.linux-x86_64
rm -rf build/lib

rm -f weathernext_3.zip
cd build && 7z a -r -snl ../weathernext_3.zip && cd ..

build/python/bin/python -m pip uninstall --root-user-action ignore -y -r build/requirements.txt
rm -f weathernext_3_min.zip
cd build && 7z a -r -snl ../weathernext_3_min.zip && cd ..

build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: setuptools wheel
build/python/bin/python -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: -r build/requirements.txt

echo SUCCESS
