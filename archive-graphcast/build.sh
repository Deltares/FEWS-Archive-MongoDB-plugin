# Copyright (c) 2025 RTI International / TVA / INFISYS

set -e

# CLEANUP
if [ -d build ]; then
  rm -rf build
fi
mkdir build

# BUILD PYTHON ENVIRONMENT
tar -xzf install/cpython*.tar.gz -C build
build/python/bin/python -m pip install --upgrade pip
build/python/bin/python -m pip install -r requirements.txt
build/python/bin/python -m pip freeze > frozen_requirements.txt

# BUILD ENVIRONMENT
tar -czf build/htpms.tar.gz -C build .

@echo SUCCESS

