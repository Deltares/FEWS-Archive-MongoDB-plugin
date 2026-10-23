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
dist/graphcast_gfs/python/bin/python -m pip install --root-user-action ignore --no-build-isolation -r requirements_graphcast_gfs.txt
dist/graphcast_gfs/python/bin/python -m pip install --root-user-action ignore --no-build-isolation models/GraphCastOperationalGfs/graphcast.zip
dist/graphcast_gfs/python/bin/python -m pip install --root-user-action ignore --no-build-isolation .
dist/graphcast_gfs/python/bin/python -m pip freeze > frozen_requirements_graphcast_gfs.txt

# ADD MODEL
\cp -rf models/GraphCastOperationalGfs dist/graphcast_gfs/

# ADD SCRIPTS
\cp -f run/graphcast_gfs.sh dist/graphcast_gfs/

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
