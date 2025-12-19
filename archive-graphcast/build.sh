# Copyright (c) 2025 RTI International / TVA / INFISYS

set -e

# CLEANUP
rm -rf build
mkdir build

# BUILD PYTHON ENVIRONMENT
tar -xzf install/cpython*.tar.gz -C build

build/python/bin/python -m pip install --root-user-action ignore --upgrade pip
build/python/bin/python -m pip install --root-user-action ignore --no-build-isolation -r requirements.txt
build/python/bin/python -m pip install --root-user-action ignore --no-build-isolation --upgrade models/GraphCastOperationalIfs/graphcast.zip
build/python/bin/python -m pip install --root-user-action ignore --no-build-isolation .
build/python/bin/python -m pip freeze > frozen_requirements.txt

# ADD MODELS
\cp -rf models/* build/

# ADD SCRIPTS
\cp -f graphcast.sh build/

# CLEANUP
rm -rf tva_graphcast.egg-info
rm -rf build/bdist.linux-x86_64
rm -rf build/lib

# BUILD ENVIRONMENT
# tar -czf graphcast.tar.gz -C build .
cd build && 7z a -r -snl ../graphcast.zip && cd ..

# UPDATE REPOSITORY
git commit -a -m "Environment Build"
git push

echo SUCCESS
