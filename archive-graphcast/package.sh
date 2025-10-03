#!/bin/bash

# Copyright (c) 2025 INFISYS INC

git clean -fdx
zip -rq graphcast.zip common decorator model sources graphcast.sh run_graphcast.py
