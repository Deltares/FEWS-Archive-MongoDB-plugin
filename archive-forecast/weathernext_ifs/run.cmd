
:: Copyright (c) 2026 INFISYS INC

:: [config_path] [weights_path]
:: run.cmd "c:\_GIT\FEWS-Archive\archive-forecast\weathernext_ifs\config\windows.xml" "c:\_GIT\FEWS-Archive\archive-forecast\weathernext_ifs\weights"

@echo off

if not exist "%~dp0.installed" (
    "%~dp0Python\python.exe" -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: setuptools wheel || goto ERROR
    "%~dp0Python\python.exe" -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: -r "%~dp0requirements.txt" || goto ERROR
    type nul > "%~dp0.installed"
)

set XLA_PYTHON_CLIENT_PREALLOCATE=false
set XLA_PYTHON_CLIENT_ALLOCATOR=platform

"%~dp0Python\python.exe" -m weathernext_ifs --config_path "%~1" --weights_path "%~2"
exit /b %errorlevel%

:ERROR
exit /b %errorlevel%
