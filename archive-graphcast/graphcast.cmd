
:: Copyright (c) 2025 INFISYS INC

:: [config_path] [model_name]
:: graphcast.cmd "c:\_GIT\FEWS-Archive\archive-graphcast\configurations\gfs_win_config.xml" "GraphCastOperationalGfs"

@echo off

set XLA_PYTHON_CLIENT_PREALLOCATE=false
set XLA_PYTHON_CLIENT_ALLOCATOR=platform

if exist "%~dp0%~2\.cdsapirc" copy "%~dp0%~2\.cdsapirc" "%UserProfile%"

"%~dp0Python\python.exe" -m pip install --upgrade "%~dp0%~2\graphcast.zip"
"%~dp0Python\python.exe" -m pip cache purge
"%~dp0Python\python.exe" -m tva_graphcast --config_path "%~1"