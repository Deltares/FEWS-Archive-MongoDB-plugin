#!/bin/bash
#
# Copyright (c) 2024 INFISYS INC
#

#[cpu|cuda] [environment_install_base_path]
#./graphcast.sh cpu /apps/fews/graphcast
set -e

miniconda="Miniconda3-latest-Linux-x86_64.sh"

if [ ! -d "$2" ]; then
  mkdir "$2"
fi

if [ ! -f "$2/$miniconda" ]; then
  rm -rf "$2/"*
  wget https://repo.anaconda.com/miniconda/$miniconda -O "$2/$miniconda"
fi

if [ ! -d "$2/miniconda3" ]; then
  bash "$2/$miniconda" -b -p "$2/miniconda3"
fi

eval "$(/"$2/miniconda3/bin/conda" shell.bash hook)"
conda init

conda config --add channels defaults
conda config --set ssl_verify false
pip config set global.trusted-host 'pypi.python.org pypi.org files.pythonhosted.org'

conda update conda -y
conda update --all -y

if [ -d "$2/miniconda3/envs/graphcast" ]; then
  conda env remove -n graphcast -y
fi

if [ -d "$2/miniconda3/envs/graphcast" ]; then
  rm -rf "$2/miniconda3/envs/graphcast"
fi

conda create -n graphcast -y
conda activate graphcast
conda install python=3.12 requests xmltodict netcdf4 zarr -y

if [ "$1" == "cuda" ]; then
  conda install "jax[cuda12]" -y
elif [ "$1" == "cpu" ]; then
  conda install "jax[cpu]" -y
fi

pip install pysolar cdsapi
conda install -c conda-forge cfgrib -y
conda env export | head -n -1 > environment.yml

echo "done"
