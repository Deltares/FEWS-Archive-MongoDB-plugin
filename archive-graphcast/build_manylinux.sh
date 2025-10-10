# Copyright (c) 2025 RTI International / TVA / INFISYS

set -e

# CLEANUP
rm -rf build wheelhouse
mkdir build wheelhouse

# BUILD PYTHON ENVIRONMENT
tar -xzf install/cpython*.tar.gz -C build

docker run --rm -v "$PWD:/io" quay.io/pypa/manylinux2014_x86_64 bash -lc "
    set -e
    /opt/python/cp313-cp313/bin/python -m pip install --upgrade pip wheel build auditwheel
    /opt/python/cp313-cp313/bin/pip wheel -r /io/requirements.txt -w /io/wheelhouse
    /opt/python/cp313-cp313/bin/pip wheel /io/models/GraphCastOperationalIfs/graphcast.zip -w /io/wheelhouse
    /opt/python/cp313-cp313/bin/pip wheel /io/. -w /io/wheelhouse --no-build-isolation
    auditwheel repair /io/wheelhouse/*.whl -w /io/wheelhouse || true
  "

build/python/bin/python -m pip install --root-user-action --upgrade pip
build/python/bin/python -m pip install --root-user-action --no-index --find-links wheelhouse -r requirements.txt
build/python/bin/python -m pip install --root-user-action --no-index --find-links wheelhouse --upgrade models/GraphCastOperationalIfs/graphcast.zip
build/python/bin/python -m pip install --root-user-action --no-index --find-links wheelhouse --no-build-isolation .
build/python/bin/python -m pip freeze > frozen_requirements.txt

# ADD MODELS
\cp -rf models/* build/

# ADD SCRIPTS
\cp -f graphcast.sh build/

# CLEANUP
rm -rf wheelhouse tva_graphcast.egg-info build/bdist.linux-x86_64 build/lib

# BUILD ENVIRONMENT
# tar -czf graphcast.tar.gz -C build .
cd build && 7z a -r -snl graphcast.zip && cd ..
mv build/graphcast.zip graphcast.zip

# UPDATE REPOSITORY
# git commit -a -m "Environment Build"
# git push

echo SUCCESS
