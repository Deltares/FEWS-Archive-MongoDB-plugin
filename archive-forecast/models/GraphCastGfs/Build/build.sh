# Copyright (c) 2025 RTI International / TVA / INFISYS

set -e

rm -rf dist/graphcast_gfs
mkdir -p dist/graphcast_gfs
rm -rf dist/graphcast_gfs_weights
mkdir -p dist/graphcast_gfs_weights

tar -xzf install/cpython*.tar.gz -C dist/graphcast_gfs

dist/graphcast_gfs/python/bin/python -m pip install --root-user-action ignore --upgrade pip
dist/graphcast_gfs/python/bin/python -m pip install --root-user-action ignore --no-build-isolation --only-binary=:all: setuptools wheel
dist/graphcast_gfs/python/bin/python -m pip install --root-user-action ignore --no-build-isolation --only-binary=:all: -r models/GraphCastGfs/requirements.txt
dist/graphcast_gfs/python/bin/python -m pip freeze > dist/graphcast_gfs/requirements.txt
dist/graphcast_gfs/python/bin/python -m pip install --root-user-action ignore --no-build-isolation --only-binary=:all: models/GraphCastGfs/model.zip
dist/graphcast_gfs/python/bin/python -m pip install --root-user-action ignore --no-build-isolation .

\cp -f models/GraphCastGfs/model.npz dist/graphcast_gfs_weights/
\cp -f models/GraphCastGfs/*.nc dist/graphcast_gfs_weights/

\cp -f models/GraphCastGfs/run.sh dist/graphcast_gfs/

rm -rf archive_forecast.egg-info
rm -rf build

rm -f dist/graphcast_gfs_weights.zip
cd dist/graphcast_gfs_weights && 7z a -r -snl ../graphcast_gfs_weights.zip && cd ../..

rm -f dist/graphcast_gfs.zip
cd dist/graphcast_gfs && 7z a -r -snl ../graphcast_gfs.zip && cd ../..

dist/graphcast_gfs/python/bin/python -m pip uninstall --root-user-action ignore -y -r dist/graphcast_gfs/requirements.txt
rm -f dist/graphcast_gfs_min.zip
cd dist/graphcast_gfs && 7z a -r -snl ../graphcast_gfs_min.zip && cd ../..

echo SUCCESS
