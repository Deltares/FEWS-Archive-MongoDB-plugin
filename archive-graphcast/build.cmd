:: Copyright (c) 2025 RTI International / TVA / INFISYS
@setlocal
@set "PROMPT=>" >nul

:: CLEANUP
if exist build rmdir "\\?\%CD%\build" /s /q || goto ERROR
mkdir build || goto ERROR

:: BUILD PYTHON ENVIRONMENT
install\7za x install\python*.zip -obuild\Python || goto ERROR
install\7za x build\Python\python*.zip -obuild\Python\Lib || goto ERROR
build\Python\python.exe install\get-pip.py --no-warn-script-location || goto ERROR

del build\Python\python*._pth /f /q || goto ERROR
del build\Python\python*.zip /f /q || goto ERROR

build\Python\python.exe -m pip install --no-warn-script-location pip || goto ERROR
build\Python\python.exe -m pip install --no-warn-script-location setuptools wheel || goto ERROR
build\Python\python.exe -m pip install --no-warn-script-location -r requirements.txt || goto ERROR
build\python\python.exe -m pip install --no-warn-script-location --no-build-isolation models\GraphCastOperationalIfs\graphcast.zip || goto ERROR
build\Python\python.exe -m pip install --no-warn-script-location --no-build-isolation . || goto ERROR
build\Python\python.exe -m pip freeze > frozen_requirements.txt || goto ERROR

:: ADD MODELS
xcopy /e /y models\* build\

:: ADD SCRIPTS
copy graphcast.cmd build\ /Y

:: CLEANUP
rmdir tva_graphcast.egg-info /s /q || goto ERROR
rmdir build\bdist.win-amd64 /s /q || goto ERROR
rmdir build\lib /s /q || goto ERROR

:: BUILD ENVIRONMENT
cd build && ( ..\install\7za a ..\graphcast.7z -r -snl || goto ERROR ) && cd ..

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
