#!/bin/bash
#
# Copyright (c) 2024 INFISYS INC
#

#[config_path] [environment_install_base_path] [model_path]
#./graphcast.sh /apps/fews/fss/201/Models/Import/GraphCast/run_info.xml /apps/fews/graphcast /apps/fews/fss/201/Modules/GraphCastOperationalGfs
set -e

miniconda="Miniconda3-latest-Linux-x86_64.sh"

if [ ! -d "$2" ]; then
  mkdir "$2"
fi

if [ ! -f "$2/$miniconda" ]; then
  rm -rf "$2/"*
  wget https://repo.anaconda.com/miniconda/$miniconda  -O "$2/$miniconda"
fi

if [ ! -d "$2/miniconda3" ]; then
  bash "$2/$miniconda" -b -p "$2/miniconda3"
fi

eval "$(/"$2/miniconda3/bin/conda" shell.bash hook)"
conda init

export XLA_PYTHON_CLIENT_PREALLOCATE=false
export XLA_PYTHON_CLIENT_ALLOCATOR=platform

conda config --add channels defaults
conda config --set solver classic

conda config --set ssl_verify false
pip config set global.trusted-host 'pypi.python.org pypi.org files.pythonhosted.org'

conda deactivate
conda update conda -y
conda update --all -y

if [ -d "$2/miniconda3/envs/graphcast" ]; then
  conda env remove -n graphcast -y
fi

if [ -d "$2/miniconda3/envs/graphcast" ]; then
  rm -rf "$2/miniconda3/envs/graphcast"
fi

conda env create -n graphcast --file environment.yml -y
conda activate graphcast

conda clean --all -y
pip install --upgrade "$3/graphcast.zip"

cp "$3/.cdsapirc" "$HOME"

python run_graphcast.py --config_path "$1"
echo "done"