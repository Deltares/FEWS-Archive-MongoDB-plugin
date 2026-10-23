:: Copyright (c) 2025 RTI International / TVA / INFISYS
@setlocal
@set "PROMPT=>" >nul

if exist build rmdir "\\?\%CD%\build" /s /q || goto ERROR
mkdir build || goto ERROR

install\7za x install\python*.zip -obuild\Python || goto ERROR
install\7za x build\Python\python*.zip -obuild\Python\Lib || goto ERROR
build\Python\python.exe install\get-pip.py --no-warn-script-location || goto ERROR

del build\Python\python*._pth /f /q || goto ERROR
del build\Python\python*.zip /f /q || goto ERROR

build\Python\python.exe -m pip install --no-warn-script-location --upgrade pip || goto ERROR
build\Python\python.exe -m pip install --no-warn-script-location setuptools wheel || goto ERROR
build\Python\python.exe -m pip install --no-warn-script-location -r requirements.txt || goto ERROR
build\Python\python.exe -m pip install --no-warn-script-location --no-build-isolation models\GraphCastOperationalIfs\weathernext.zip || goto ERROR
build\Python\python.exe -m pip install --no-warn-script-location --no-build-isolation models\GraphCastOperationalGfs\graphcast.zip || goto ERROR
build\Python\python.exe -m pip install --no-warn-script-location --no-build-isolation -e . || goto ERROR
build\Python\python.exe -m pip freeze --exclude-editable > frozen_requirements.txt || goto ERROR

if exist archive_forecast.egg-info rmdir archive_forecast.egg-info /s /q

git commit -a -m "Environment Build"
git push || goto ERROR

@echo SUCCESS
@goto END

:ERROR
@echo ERROR
@goto END

:END
@endlocal
