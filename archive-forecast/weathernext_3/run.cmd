:: Copyright (c) 2026 INFISYS INC

:: [config_path]
:: run.cmd "c:\_GIT\FEWS-Archive\archive-forecast\weathernext_3\config\windows.xml"

@echo off

if not exist "%~dp0.installed" (
    "%~dp0Python\python.exe" -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: setuptools wheel || goto ERROR
    "%~dp0Python\python.exe" -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: -r "%~dp0requirements.txt" || goto ERROR
    type nul > "%~dp0.installed"
)

set "GOOGLE_APPLICATION_CREDENTIALS=%~dp0weathernext3tva.json"

"%~dp0Python\python.exe" -m weathernext_3 --config_path "%~1"
exit /b %errorlevel%

:ERROR
exit /b %errorlevel%
