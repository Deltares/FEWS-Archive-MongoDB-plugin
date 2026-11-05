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
build\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: model\model.zip || goto ERROR
build\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation ..\archive_forecast || goto ERROR
build\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation . || goto ERROR

copy run.cmd build\ /Y || goto ERROR

if exist src\graphcast_gfs.egg-info rmdir src\graphcast_gfs.egg-info /s /q
if exist build\bdist.win-amd64 rmdir build\bdist.win-amd64 /s /q
if exist build\lib rmdir build\lib /s /q
if exist ..\archive_forecast\src\archive_forecast.egg-info rmdir ..\archive_forecast\src\archive_forecast.egg-info /s /q
if exist ..\archive_forecast\build\bdist.win-amd64 rmdir ..\archive_forecast\build\bdist.win-amd64 /s /q
if exist ..\archive_forecast\build\lib rmdir ..\archive_forecast\build\lib /s /q

if exist graphcast_gfs_weights.zip del graphcast_gfs_weights.zip /f /q
pushd weights && ( ..\..\install\7za a ..\graphcast_gfs_weights.zip -r -snl || goto ERROR ) & popd

if exist graphcast_gfs.zip del graphcast_gfs.zip /f /q
pushd build && ( ..\..\install\7za a ..\graphcast_gfs.zip -r -snl || goto ERROR ) & popd

build\Python\python.exe -m pip uninstall --root-user-action ignore -y -r build\requirements.txt
if exist graphcast_gfs_min.zip del graphcast_gfs_min.zip /f /q
pushd build && ( ..\..\install\7za a ..\graphcast_gfs_min.zip -r -snl || goto ERROR ) & popd

build\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: setuptools wheel || goto ERROR
build\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: -r build\requirements.txt || goto ERROR
build\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --no-deps -e ..\archive_forecast || goto ERROR
if exist ..\archive_forecast\src\archive_forecast.egg-info rmdir ..\archive_forecast\src\archive_forecast.egg-info /s /q
build\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --no-deps -e . || goto ERROR
if exist src\graphcast_gfs.egg-info rmdir src\graphcast_gfs.egg-info /s /q
build\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --only-binary=:all: --group dev || goto ERROR

@echo SUCCESS
@goto END

:ERROR
@echo ERROR
@goto END

:END
@endlocal
