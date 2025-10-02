::Copyright (c) 2025 INFISYS INC

::[config_path] [environment_install_base_path] [model_path]
::graphcast.cmd "c:\_GIT\FEWS-Archive\archive-graphcast\configurations\gfs_win_config.xml" "c:\_GIT\FEWS-Archive\archive-graphcast\build" "c:\_GIT\FEWS-Archive\archive-graphcast\models\GraphCastOperationalGfs"
@echo off
set XLA_PYTHON_CLIENT_PREALLOCATE=false
set XLA_PYTHON_CLIENT_ALLOCATOR=platform

copy "%~3\.cdsapirc" "%UserProfile%"

"%~2\Python\python.exe" -m pip install --upgrade "%~3\graphcast.zip"
"%~2\Python\python.exe" -m pip cache purge
"%~2\Python\python.exe" run_graphcast.py --config_path "%~1"