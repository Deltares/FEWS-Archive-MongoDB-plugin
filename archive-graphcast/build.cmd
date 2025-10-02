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

build\Python\python.exe -m pip install --upgrade pip --no-warn-script-location || goto ERROR
build\Python\python.exe -m pip install setuptools --no-warn-script-location || goto ERROR
build\Python\python.exe -m pip install -r requirements.txt --no-warn-script-location || goto ERROR
build\Python\python.exe -m pip freeze > frozen_requirements.txt || goto ERROR

:: BUILD ENVIRONMENT
cd build && ( ..\install\7za a verification.7z -r -mmt16 || goto ERROR ) && cd ..
move build\verification.7z verification.7z || goto ERROR

:: CLEANUP
if exist build rmdir "\\?\%CD%\build" /s /q || goto ERROR

:: UPDATE REPOSITORY
::git commit -a -m "Environment Build"
::git push || goto ERROR

@echo SUCCESS
@goto END

:ERROR
@echo ERROR
@goto END

:END
@endlocal
