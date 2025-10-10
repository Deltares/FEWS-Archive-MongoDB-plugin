# Copyright (c) 2025 RTI International / TVA / INFISYS

set -e

# CLEANUP
rm -rf build wheelhouse
mkdir build wheelhouse

# BUILD PYTHON ENVIRONMENT
tar -xzf install/old/cpython-3.12*.tar.gz -C build

docker run --rm -v "$PWD:/io" quay.io/pypa/manylinux2014_x86_64 bash -lc '
  set -e
  /opt/python/cp312-cp312/bin/python -m pip install --upgrade pip wheel build setuptools auditwheel
  /opt/python/cp312-cp312/bin/pip wheel -r /io/requirements.txt -w /io/wheelhouse
  /opt/python/cp312-cp312/bin/pip wheel /io/models/GraphCastOperationalIfs/graphcast.zip -w /io/wheelhouse
  /opt/python/cp312-cp312/bin/pip wheel /io/. -w /io/wheelhouse --no-deps --no-build-isolation
  auditwheel repair /io/wheelhouse/*.whl -w /io/wheelhouse || true

  #cp -a /usr/lib64/libstdc++.so.6.* /io/build/python/lib/
  #cd /io/build/python/lib
  #ln -sfn "$(ls -1 libstdc++.so.6.* | sort | tail -n1)" libstdc++.so.6
'

build/python/bin/python -m pip install --root-user-action ignore --upgrade pip
build/python/bin/python -m pip install --root-user-action ignore --no-index --find-links wheelhouse -r requirements.txt
build/python/bin/python -m pip install --root-user-action ignore --no-index --find-links wheelhouse --upgrade models/GraphCastOperationalIfs/graphcast.zip
build/python/bin/python -m pip install --root-user-action ignore --no-index --find-links wheelhouse --no-build-isolation .
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
