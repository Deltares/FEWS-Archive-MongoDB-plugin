::
:: Copyright (c) 2024 INFISYS INC
::

@echo off
copy ".\environments\%~1" ".\environment.yml" /Y
if exist ".\decorator\__pycache__" rmdir .\decorator\__pycache__ /S /Q
if exist ".\model\__pycache__" rmdir .\model\__pycache__ /S /Q
if exist ".\sources\__pycache__" rmdir .\sources\__pycache__ /S /Q
if exist ".\RunGraphCast.zip" del .\RunGraphCast.zip /Q
7z a -tzip RunGraphCast.zip decorator model sources environment.yml graphcast.cmd graphcast.sh graphcast_env.cmd graphcast_env.sh run_graphcast.py
del ".\environment.yml" /Q /F