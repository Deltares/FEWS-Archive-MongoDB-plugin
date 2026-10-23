:: Copyright (c) 2025 INFISYS INC

:: [config_path]
:: run.cmd "c:\_GIT\FEWS-Archive\archive-forecast\configurations\gfs_win_config.xml"

@echo off

set XLA_PYTHON_CLIENT_PREALLOCATE=false
set XLA_PYTHON_CLIENT_ALLOCATOR=platform

"%~dp0Python\python.exe" -m archive_forecast --provider graphcast_gfs --assets_path "%~dp0GraphCastOperationalGfs" --config_path "%~1"
