:: Copyright (c) 2025 INFISYS INC

:: [config_path]
:: run.cmd "c:\_GIT\FEWS-Archive\archive-forecast\configurations\ifs_win_config.xml"

@echo off

set XLA_PYTHON_CLIENT_PREALLOCATE=false
set XLA_PYTHON_CLIENT_ALLOCATOR=platform
set CDSAPI_RC=%~dp0GraphCastOperationalIfs\.cdsapirc

"%~dp0Python\python.exe" -m archive_forecast --provider weathernext_ifs --assets_path "%~dp0GraphCastOperationalIfs" --config_path "%~1"
