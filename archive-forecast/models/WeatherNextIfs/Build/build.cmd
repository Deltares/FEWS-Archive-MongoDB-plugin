:: Copyright (c) 2025 RTI International / TVA / INFISYS
@setlocal
@set "PROMPT=>" >nul

if exist dist\weathernext_ifs rmdir "\\?\%CD%\dist\weathernext_ifs" /s /q || goto ERROR
mkdir dist\weathernext_ifs || goto ERROR
if exist dist\weathernext_ifs_weights rmdir "\\?\%CD%\dist\weathernext_ifs_weights" /s /q || goto ERROR
mkdir dist\weathernext_ifs_weights || goto ERROR

install\7za x install\python*.zip -odist\weathernext_ifs\Python || goto ERROR
install\7za x dist\weathernext_ifs\Python\python*.zip -odist\weathernext_ifs\Python\Lib || goto ERROR
dist\weathernext_ifs\Python\python.exe install\get-pip.py --no-warn-script-location || goto ERROR

del dist\weathernext_ifs\Python\python*._pth /f /q || goto ERROR
del dist\weathernext_ifs\Python\python*.zip /f /q || goto ERROR

dist\weathernext_ifs\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --upgrade pip || goto ERROR
dist\weathernext_ifs\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: setuptools wheel || goto ERROR
dist\weathernext_ifs\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: -r models\WeatherNextIfs\requirements.txt || goto ERROR
dist\weathernext_ifs\Python\python.exe -m pip freeze > dist\weathernext_ifs\requirements.txt || goto ERROR
dist\weathernext_ifs\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: models\WeatherNextIfs\model.zip || goto ERROR
dist\weathernext_ifs\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation . || goto ERROR

copy models\WeatherNextIfs\model.npz dist\weathernext_ifs_weights\ /Y || goto ERROR
copy models\WeatherNextIfs\*.nc dist\weathernext_ifs_weights\ /Y || goto ERROR

copy models\WeatherNextIfs\.cdsapirc dist\weathernext_ifs\ /Y || goto ERROR
copy models\WeatherNextIfs\run.cmd dist\weathernext_ifs\ /Y || goto ERROR

if exist archive_forecast.egg-info rmdir archive_forecast.egg-info /s /q
if exist build rmdir build /s /q

if exist dist\weathernext_ifs_weights.7z del dist\weathernext_ifs_weights.7z /f /q
pushd dist\weathernext_ifs_weights && ( ..\..\install\7za a ..\weathernext_ifs_weights.7z -r -snl || goto ERROR ) & popd

if exist dist\weathernext_ifs.7z del dist\weathernext_ifs.7z /f /q
pushd dist\weathernext_ifs && ( ..\..\install\7za a ..\weathernext_ifs.7z -r -snl || goto ERROR ) & popd

dist\weathernext_ifs\Python\python.exe -m pip uninstall --root-user-action ignore -y -r dist\weathernext_ifs\requirements.txt
if exist dist\weathernext_ifs_min.7z del dist\weathernext_ifs_min.7z /f /q
pushd dist\weathernext_ifs && ( ..\..\install\7za a ..\weathernext_ifs_min.7z -r -snl || goto ERROR ) & popd

@echo SUCCESS
@goto END

:ERROR
@echo ERROR
@goto END

:END
@endlocal
