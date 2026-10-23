# Copyright (c) 2025 RTI International / TVA / INFISYS

set -e

rm -rf dist/weathernext_ifs
mkdir -p dist/weathernext_ifs
rm -rf dist/weathernext_ifs_weights
mkdir -p dist/weathernext_ifs_weights

tar -xzf install/cpython*.tar.gz -C dist/weathernext_ifs

dist/weathernext_ifs/python/bin/python -m pip install --root-user-action ignore --upgrade pip
dist/weathernext_ifs/python/bin/python -m pip install --root-user-action ignore --no-build-isolation --only-binary=:all: setuptools wheel
dist/weathernext_ifs/python/bin/python -m pip install --root-user-action ignore --no-build-isolation --only-binary=:all: -r models/WeatherNextIfs/requirements.txt
dist/weathernext_ifs/python/bin/python -m pip freeze > dist/weathernext_ifs/requirements.txt
dist/weathernext_ifs/python/bin/python -m pip install --root-user-action ignore --no-build-isolation --only-binary=:all: models/WeatherNextIfs/model.zip
dist/weathernext_ifs/python/bin/python -m pip install --root-user-action ignore --no-build-isolation .

\cp -f models/WeatherNextIfs/model.npz dist/weathernext_ifs_weights/
\cp -f models/WeatherNextIfs/*.nc dist/weathernext_ifs_weights/

\cp -f models/WeatherNextIfs/.cdsapirc dist/weathernext_ifs/
\cp -f models/WeatherNextIfs/run.sh dist/weathernext_ifs/

rm -rf archive_forecast.egg-info
rm -rf build

rm -f dist/weathernext_ifs_weights.zip
cd dist/weathernext_ifs_weights && 7z a -r -snl ../weathernext_ifs_weights.zip && cd ../..

rm -f dist/weathernext_ifs.zip
cd dist/weathernext_ifs && 7z a -r -snl ../weathernext_ifs.zip && cd ../..

dist/weathernext_ifs/python/bin/python -m pip uninstall --root-user-action ignore -y -r dist/weathernext_ifs/requirements.txt
rm -f dist/weathernext_ifs_min.zip
cd dist/weathernext_ifs && 7z a -r -snl ../weathernext_ifs_min.zip && cd ../..

echo SUCCESS
