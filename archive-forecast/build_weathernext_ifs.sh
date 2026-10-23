# Copyright (c) 2025 RTI International / TVA / INFISYS

# IFS WeatherNext bundle: dist/weathernext_ifs -> dist/weathernext_ifs.zip (model.zip needs git on PATH)

set -e

# CLEANUP
rm -rf dist/weathernext_ifs
mkdir -p dist/weathernext_ifs

# BUILD PYTHON ENVIRONMENT
tar -xzf install/cpython*.tar.gz -C dist/weathernext_ifs

dist/weathernext_ifs/python/bin/python -m pip install --root-user-action ignore --upgrade pip
dist/weathernext_ifs/python/bin/python -m pip install --root-user-action ignore setuptools wheel
dist/weathernext_ifs/python/bin/python -m pip install --root-user-action ignore --no-build-isolation -r requirements_weathernext_ifs.txt
dist/weathernext_ifs/python/bin/python -m pip install --root-user-action ignore --no-build-isolation models/GraphCastOperationalIfs/weathernext.zip
dist/weathernext_ifs/python/bin/python -m pip install --root-user-action ignore --no-build-isolation .
dist/weathernext_ifs/python/bin/python -m pip freeze > frozen_requirements_weathernext_ifs.txt

# ADD MODEL
\cp -rf models/GraphCastOperationalIfs dist/weathernext_ifs/

# ADD SCRIPTS
\cp -f run/weathernext_ifs.sh dist/weathernext_ifs/

# CLEANUP
rm -rf archive_forecast.egg-info
rm -rf build/bdist.linux-x86_64
rm -rf build/lib
rm -f dist/weathernext_ifs.zip

# BUILD ENVIRONMENT
cd dist/weathernext_ifs && 7z a -r -snl ../weathernext_ifs.zip && cd ../..

# UPDATE REPOSITORY
git commit -a -m "Environment Build"
git push

echo SUCCESS
