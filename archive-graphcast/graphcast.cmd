
::Copyright (c) 2025 INFISYS INC

::[config_path] [model_path]
::.\graphcast.cmd "c:\_GIT\FEWS-Archive\archive-graphcast\configurations\gfs_win_config.xml" "GraphCastOperationalGfs"

@echo off

set XLA_PYTHON_CLIENT_PREALLOCATE=false
set XLA_PYTHON_CLIENT_ALLOCATOR=platform

copy "%~2\.cdsapirc" "%UserProfile%"

"Python\python.exe" -m pip install --upgrade "%~2\graphcast.zip"
"Python\python.exe" -m pip cache purge
"Python\python.exe" -m tva_graphcast --config_path "%~1"