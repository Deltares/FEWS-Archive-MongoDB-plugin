:: Copyright (c) 2025 RTI International / TVA / INFISYS
@setlocal
@set "PROMPT=>" >nul

if exist dist\graphcast_gfs rmdir "\\?\%CD%\dist\graphcast_gfs" /s /q || goto ERROR
mkdir dist\graphcast_gfs || goto ERROR
if exist dist\graphcast_gfs_weights rmdir "\\?\%CD%\dist\graphcast_gfs_weights" /s /q || goto ERROR
mkdir dist\graphcast_gfs_weights || goto ERROR

install\7za x install\python*.zip -odist\graphcast_gfs\Python || goto ERROR
install\7za x dist\graphcast_gfs\Python\python*.zip -odist\graphcast_gfs\Python\Lib || goto ERROR
dist\graphcast_gfs\Python\python.exe install\get-pip.py --no-warn-script-location || goto ERROR

del dist\graphcast_gfs\Python\python*._pth /f /q || goto ERROR
del dist\graphcast_gfs\Python\python*.zip /f /q || goto ERROR

dist\graphcast_gfs\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --upgrade pip || goto ERROR
dist\graphcast_gfs\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: setuptools wheel || goto ERROR
dist\graphcast_gfs\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: -r models\GraphCastGfs\requirements.txt || goto ERROR
dist\graphcast_gfs\Python\python.exe -m pip freeze > dist\graphcast_gfs\requirements.txt || goto ERROR
dist\graphcast_gfs\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: models\GraphCastGfs\model.zip || goto ERROR
dist\graphcast_gfs\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation . || goto ERROR

copy models\GraphCastGfs\model.npz dist\graphcast_gfs_weights\ /Y || goto ERROR
copy models\GraphCastGfs\*.nc dist\graphcast_gfs_weights\ /Y || goto ERROR

copy models\GraphCastGfs\run.cmd dist\graphcast_gfs\ /Y || goto ERROR

if exist archive_forecast.egg-info rmdir archive_forecast.egg-info /s /q
if exist build rmdir build /s /q

if exist dist\graphcast_gfs_weights.7z del dist\graphcast_gfs_weights.7z /f /q
pushd dist\graphcast_gfs_weights && ( ..\..\install\7za a ..\graphcast_gfs_weights.7z -r -snl || goto ERROR ) & popd

if exist dist\graphcast_gfs.7z del dist\graphcast_gfs.7z /f /q
pushd dist\graphcast_gfs && ( ..\..\install\7za a ..\graphcast_gfs.7z -r -snl || goto ERROR ) & popd

dist\graphcast_gfs\Python\python.exe -m pip uninstall --root-user-action ignore -y -r dist\graphcast_gfs\requirements.txt
if exist dist\graphcast_gfs_min.7z del dist\graphcast_gfs_min.7z /f /q
pushd dist\graphcast_gfs && ( ..\..\install\7za a ..\graphcast_gfs_min.7z -r -snl || goto ERROR ) & popd

@echo SUCCESS
@goto END

:ERROR
@echo ERROR
@goto END

:END
@endlocal
