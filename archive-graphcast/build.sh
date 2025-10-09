# Copyright (c) 2025 RTI International / TVA / INFISYS

set -e

# CLEANUP
if [ -d build ]; then rm -rf build; fi
mkdir build

# BUILD PYTHON ENVIRONMENT
tar -xzf install/cpython*.tar.gz -C build

build/python/bin/python -m pip install --upgrade pip
build/python/bin/python -m pip install -r requirements.txt
build/python/bin/python -m pip install --upgrade models/GraphCastOperationalIfs/graphcast.zip
build/python/bin/python -m pip install . --no-build-isolation
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
# tar -czf tva_graphcast.tar.gz -C build .
cd build && 7z a -r graphcast.zip && cd ..
mv build/graphcast.zip graphcast.zip

# UPDATE REPOSITORY
git commit -a -m "Environment Build"
git push

echo SUCCESS
