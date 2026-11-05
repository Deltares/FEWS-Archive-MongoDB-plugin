:: Copyright (c) 2026 INFISYS INC
@setlocal
@set "FAILED=0"

pushd archive_forecast || goto ERROR
build\Python\python.exe -m pytest --cov --cov-report=term-missing || set "FAILED=1"
popd

pushd graphcast_gfs || goto ERROR
build\Python\python.exe -m pytest --cov --cov-report=term-missing || set "FAILED=1"
popd

pushd weathernext_ifs || goto ERROR
build\Python\python.exe -m pytest --cov --cov-report=term-missing || set "FAILED=1"
popd

pushd weathernext_3 || goto ERROR
build\Python\python.exe -m pytest --cov --cov-report=term-missing || set "FAILED=1"
popd

@if "%FAILED%"=="1" goto ERROR

@echo SUCCESS
@goto END

:ERROR
@echo ERROR
@endlocal
@exit /b 1

:END
@endlocal
