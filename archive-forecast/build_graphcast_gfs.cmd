:: Copyright (c) 2025 RTI International / TVA / INFISYS
@setlocal
@set "PROMPT=>" >nul

:: GFS GraphCast bundle: dist\graphcast_gfs -> dist\graphcast_gfs.7z

:: CLEANUP
if exist dist\graphcast_gfs rmdir "\\?\%CD%\dist\graphcast_gfs" /s /q || goto ERROR
mkdir dist\graphcast_gfs || goto ERROR

:: BUILD PYTHON ENVIRONMENT
install\7za x install\python*.zip -odist\graphcast_gfs\Python || goto ERROR
install\7za x dist\graphcast_gfs\Python\python*.zip -odist\graphcast_gfs\Python\Lib || goto ERROR
dist\graphcast_gfs\Python\python.exe install\get-pip.py --no-warn-script-location || goto ERROR

del dist\graphcast_gfs\Python\python*._pth /f /q || goto ERROR
del dist\graphcast_gfs\Python\python*.zip /f /q || goto ERROR

dist\graphcast_gfs\Python\python.exe -m pip install --no-warn-script-location --upgrade pip || goto ERROR
dist\graphcast_gfs\Python\python.exe -m pip install --no-warn-script-location setuptools wheel || goto ERROR
dist\graphcast_gfs\Python\python.exe -m pip install --no-warn-script-location -r requirements_graphcast_gfs.txt || goto ERROR
dist\graphcast_gfs\Python\python.exe -m pip install --no-warn-script-location --no-build-isolation models\GraphCastOperationalGfs\graphcast.zip || goto ERROR
dist\graphcast_gfs\Python\python.exe -m pip install --no-warn-script-location --no-build-isolation . || goto ERROR
dist\graphcast_gfs\Python\python.exe -m pip freeze > frozen_requirements_graphcast_gfs.txt || goto ERROR

:: ADD MODEL
xcopy /e /y /i models\GraphCastOperationalGfs dist\graphcast_gfs\GraphCastOperationalGfs || goto ERROR

:: ADD SCRIPTS
copy run\graphcast_gfs.cmd dist\graphcast_gfs\ /Y || goto ERROR

:: CLEANUP
if exist archive_forecast.egg-info rmdir archive_forecast.egg-info /s /q
if exist build\bdist.win-amd64 rmdir build\bdist.win-amd64 /s /q
if exist build\lib rmdir build\lib /s /q
if exist dist\graphcast_gfs.7z del dist\graphcast_gfs.7z /f /q

:: BUILD ENVIRONMENT
pushd dist\graphcast_gfs && ( ..\..\install\7za a ..\graphcast_gfs.7z -r -snl || goto ERROR ) && popd

:: UPDATE REPOSITORY
git commit -a -m "Environment Build"
git push || goto ERROR

@echo SUCCESS
@goto END

:ERROR
@echo ERROR
@goto END

:END
@endlocal
