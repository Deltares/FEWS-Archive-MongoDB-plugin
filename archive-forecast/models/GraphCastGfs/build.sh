# Copyright (c) 2025 RTI International / TVA / INFISYS

# GFS GraphCast bundle: dist/graphcast_gfs -> dist/graphcast_gfs.zip

set -e

# CLEANUP
rm -rf dist/graphcast_gfs
mkdir -p dist/graphcast_gfs

# BUILD PYTHON ENVIRONMENT
tar -xzf install/cpython*.tar.gz -C dist/graphcast_gfs

dist/graphcast_gfs/python/bin/python -m pip install --root-user-action ignore --upgrade pip
dist/graphcast_gfs/python/bin/python -m pip install --root-user-action ignore setuptools wheel
dist/graphcast_gfs/python/bin/python -m pip install --root-user-action ignore --no-build-isolation -r models/GraphCastGfs/requirements.txt
dist/graphcast_gfs/python/bin/python -m pip install --root-user-action ignore --no-build-isolation models/GraphCastGfs/model.zip
dist/graphcast_gfs/python/bin/python -m pip install --root-user-action ignore --no-build-isolation .
dist/graphcast_gfs/python/bin/python -m pip freeze > models/GraphCastGfs/frozen_requirements.txt

# ADD MODEL
mkdir -p dist/graphcast_gfs/GraphCastGfs
\cp -f models/GraphCastGfs/model.npz models/GraphCastGfs/*.nc dist/graphcast_gfs/GraphCastGfs/

# ADD SCRIPTS
\cp -f models/GraphCastGfs/run.sh dist/graphcast_gfs/

# CLEANUP
rm -rf archive_forecast.egg-info
rm -rf build/bdist.linux-x86_64
rm -rf build/lib
rm -f dist/graphcast_gfs.zip

# BUILD ENVIRONMENT
cd dist/graphcast_gfs && 7z a -r -snl ../graphcast_gfs.zip && cd ../..

# UPDATE REPOSITORY
git commit -a -m "Environment Build"
git push

echo SUCCESS
