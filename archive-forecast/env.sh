# Copyright (c) 2025 RTI International / TVA / INFISYS

# Development environment: every provider's dependencies, archive_forecast installed editable (runs the working tree)

set -e

# CLEANUP
rm -rf build
mkdir build

# BUILD PYTHON ENVIRONMENT
tar -xzf install/cpython*.tar.gz -C build

build/python/bin/python -m pip install --root-user-action ignore --upgrade pip
build/python/bin/python -m pip install --root-user-action ignore setuptools wheel
build/python/bin/python -m pip install --root-user-action ignore --no-build-isolation -r requirements.txt
build/python/bin/python -m pip install --root-user-action ignore --no-build-isolation models/GraphCastOperationalIfs/weathernext.zip
build/python/bin/python -m pip install --root-user-action ignore --no-build-isolation models/GraphCastOperationalGfs/graphcast.zip
build/python/bin/python -m pip install --root-user-action ignore --no-build-isolation -e .
build/python/bin/python -m pip freeze --exclude-editable > frozen_requirements.txt

# CLEANUP
rm -rf archive_forecast.egg-info

# UPDATE REPOSITORY
git commit -a -m "Environment Build"
git push

echo SUCCESS
