# Copyright (c) 2025 RTI International / TVA / INFISYS

set -ex

# CLEANUP
rm -rf build
mkdir build

# BUILD PYTHON ENVIRONMENT
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
bash miniconda.sh -b -p build/miniconda
rm -f miniconda.sh
source build/miniconda/etc/profile.d/conda.sh

conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main
conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r

conda env create -f environment.yml
conda activate env
conda env create -y -n env
conda activate env

pip install models/GraphCastOperationalIfs/graphcast.zip
pip install .

conda env export > frozen_environment.yml
conda deactivate

# ADD MODELS
\cp -rf models/* build/

# ADD SCRIPTS
\cp -f graphcast.sh build/

# CLEANUP
rm -rf tva_graphcast.egg-info

# BUILD ENVIRONMENT
# tar -czf graphcast.tar.gz -C build .
cd build && 7z a -r -snl ../graphcast.zip && cd ..

# UPDATE REPOSITORY
# git commit -a -m "Environment Build"
# git push

echo SUCCESS
