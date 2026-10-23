:: Copyright (c) 2025 RTI International / TVA / INFISYS
@setlocal
@set "PROMPT=>" >nul

:: IFS WeatherNext bundle: dist\weathernext_ifs -> dist\weathernext_ifs.7z (weathernext.zip needs git on PATH)

:: CLEANUP
if exist dist\weathernext_ifs rmdir "\\?\%CD%\dist\weathernext_ifs" /s /q || goto ERROR
mkdir dist\weathernext_ifs || goto ERROR

:: BUILD PYTHON ENVIRONMENT
install\7za x install\python*.zip -odist\weathernext_ifs\Python || goto ERROR
install\7za x dist\weathernext_ifs\Python\python*.zip -odist\weathernext_ifs\Python\Lib || goto ERROR
dist\weathernext_ifs\Python\python.exe install\get-pip.py --no-warn-script-location || goto ERROR

del dist\weathernext_ifs\Python\python*._pth /f /q || goto ERROR
del dist\weathernext_ifs\Python\python*.zip /f /q || goto ERROR

dist\weathernext_ifs\Python\python.exe -m pip install --no-warn-script-location --upgrade pip || goto ERROR
dist\weathernext_ifs\Python\python.exe -m pip install --no-warn-script-location setuptools wheel || goto ERROR
dist\weathernext_ifs\Python\python.exe -m pip install --no-warn-script-location -r requirements_weathernext_ifs.txt || goto ERROR
dist\weathernext_ifs\Python\python.exe -m pip install --no-warn-script-location --no-build-isolation models\GraphCastOperationalIfs\weathernext.zip || goto ERROR
dist\weathernext_ifs\Python\python.exe -m pip install --no-warn-script-location --no-build-isolation . || goto ERROR
dist\weathernext_ifs\Python\python.exe -m pip freeze > frozen_requirements_weathernext_ifs.txt || goto ERROR

:: ADD MODEL
xcopy /e /y /i models\GraphCastOperationalIfs dist\weathernext_ifs\GraphCastOperationalIfs || goto ERROR

:: ADD SCRIPTS
copy run\weathernext_ifs.cmd dist\weathernext_ifs\ /Y || goto ERROR

:: CLEANUP
if exist archive_forecast.egg-info rmdir archive_forecast.egg-info /s /q
if exist build\bdist.win-amd64 rmdir build\bdist.win-amd64 /s /q
if exist build\lib rmdir build\lib /s /q
if exist dist\weathernext_ifs.7z del dist\weathernext_ifs.7z /f /q

:: BUILD ENVIRONMENT
pushd dist\weathernext_ifs && ( ..\..\install\7za a ..\weathernext_ifs.7z -r -snl || goto ERROR ) && popd

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
