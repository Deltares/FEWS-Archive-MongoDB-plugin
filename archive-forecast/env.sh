# Copyright (c) 2025 RTI International / TVA / INFISYS

# Development environment: every provider's dependencies, archive_forecast installed editable (runs the working tree)

set -e

# CLEANUP
rm -rf env
mkdir env

# BUILD PYTHON ENVIRONMENT
tar -xzf install/cpython*.tar.gz -C env

env/python/bin/python -m pip install --root-user-action ignore --upgrade pip
env/python/bin/python -m pip install --root-user-action ignore setuptools wheel
env/python/bin/python -m pip install --root-user-action ignore --no-build-isolation -r requirements.txt
env/python/bin/python -m pip install --root-user-action ignore --no-build-isolation models/WeatherNextIfs/model.zip
env/python/bin/python -m pip install --root-user-action ignore --no-build-isolation models/GraphCastGfs/model.zip
env/python/bin/python -m pip install --root-user-action ignore --no-build-isolation -e .
env/python/bin/python -m pip freeze --exclude-editable > frozen_requirements.txt

# CLEANUP
rm -rf archive_forecast.egg-info

# UPDATE REPOSITORY
git commit -a -m "Environment Build"
git push

echo SUCCESS
