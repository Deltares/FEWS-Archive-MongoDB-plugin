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
build\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: setuptools wheel || goto ERROR
build\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: -r requirements.txt || goto ERROR
build\Python\python.exe -m pip freeze > build\requirements.txt || goto ERROR
build\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation ..\archive_forecast || goto ERROR
build\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation . || goto ERROR

copy run.cmd build\ /Y || goto ERROR
copy weathernext3tva.json build\ /Y || goto ERROR

if exist src\weathernext_3.egg-info rmdir src\weathernext_3.egg-info /s /q
if exist build\bdist.win-amd64 rmdir build\bdist.win-amd64 /s /q
if exist build\lib rmdir build\lib /s /q
if exist ..\archive_forecast\src\archive_forecast.egg-info rmdir ..\archive_forecast\src\archive_forecast.egg-info /s /q
if exist ..\archive_forecast\build\bdist.win-amd64 rmdir ..\archive_forecast\build\bdist.win-amd64 /s /q
if exist ..\archive_forecast\build\lib rmdir ..\archive_forecast\build\lib /s /q

if exist weathernext_3.zip del weathernext_3.zip /f /q
pushd build && ( ..\..\install\7za a ..\weathernext_3.zip -r -snl || goto ERROR ) & popd

build\Python\python.exe -m pip uninstall --root-user-action ignore -y -r build\requirements.txt
if exist weathernext_3_min.zip del weathernext_3_min.zip /f /q
pushd build && ( ..\..\install\7za a ..\weathernext_3_min.zip -r -snl || goto ERROR ) & popd

build\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: setuptools wheel || goto ERROR
build\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: -r build\requirements.txt || goto ERROR
build\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --no-deps -e ..\archive_forecast || goto ERROR
if exist ..\archive_forecast\src\archive_forecast.egg-info rmdir ..\archive_forecast\src\archive_forecast.egg-info /s /q
build\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --no-deps -e . || goto ERROR
if exist src\weathernext_3.egg-info rmdir src\weathernext_3.egg-info /s /q
build\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --only-binary=:all: --group dev || goto ERROR
build\Python\python.exe -m pytest --cov --cov-report=term-missing || goto ERROR

@echo SUCCESS
@goto END

:ERROR
@echo ERROR
@endlocal
@exit /b 1

:END
@endlocal
