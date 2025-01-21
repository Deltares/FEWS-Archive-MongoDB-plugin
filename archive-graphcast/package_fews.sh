#!/bin/bash

#
# Copyright (c) 2024 INFISYS INC
#

cp -f "./environments/$1" "./environment.yml"

if [ -d "./common/__pycache__" ]; then
    rm -rf ./common/__pycache__
fi

if [ -d "./decorator/__pycache__" ]; then
    rm -rf ./decorator/__pycache__
fi

if [ -d "./model/__pycache__" ]; then
    rm -rf ./model/__pycache__
fi

if [ -d "./sources/__pycache__" ]; then
    rm -rf ./sources/__pycache__
fi

zip -rq RunGraphCast.zip common decorator model sources environment.yml graphcast.cmd graphcast.sh graphcast_env.cmd graphcast_env.sh run_graphcast.py
rm -f "./environment.yml"