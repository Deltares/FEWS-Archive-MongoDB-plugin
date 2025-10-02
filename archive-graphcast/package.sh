#!/bin/bash

# Copyright (c) 2025 INFISYS INC

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

zip -rq graphcast.zip common decorator model sources graphcast.sh run_graphcast.py
