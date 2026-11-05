:: Copyright (c) 2026 INFISYS INC
@setlocal
@set "PROMPT=>" >nul

if exist build rmdir "\\?\%CD%\build" /s /q || goto ERROR
mkdir build || goto ERROR

..\install\7za x ..\install\python*.zip -obuild\Python || goto ERROR
..\install\7za x build\Python\python*.zip -obuild\Python\Lib || goto ERROR

build\Python\python.exe ..\install\get-pip.py --no-warn-script-location || goto ERROR

del build\Python\python*._pth /f /q || goto ERROR
del build\Python\python*.zip /f /q || goto ERROR

build\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --upgrade pip || goto ERROR
build\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: setuptools || goto ERROR
build\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: -r requirements.txt || goto ERROR
build\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: --group dev || goto ERROR

if exist src\archive_forecast.egg-info rmdir src\archive_forecast.egg-info /s /q
if exist build\bdist.win-amd64 rmdir build\bdist.win-amd64 /s /q
if exist build\lib rmdir build\lib /s /q

build\Python\python.exe -m pytest --cov --cov-report=term-missing || goto ERROR

@echo SUCCESS
@goto END

:ERROR
@echo ERROR
@endlocal
@exit /b 1

:END
@endlocal
