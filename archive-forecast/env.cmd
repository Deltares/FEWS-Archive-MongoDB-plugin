:: Copyright (c) 2025 RTI International / TVA / INFISYS
@setlocal
@set "PROMPT=>" >nul

if exist env rmdir "\\?\%CD%\env" /s /q || goto ERROR
mkdir env || goto ERROR

install\7za x install\python*.zip -oenv\Python || goto ERROR
install\7za x env\Python\python*.zip -oenv\Python\Lib || goto ERROR
env\Python\python.exe install\get-pip.py --no-warn-script-location || goto ERROR

del env\Python\python*._pth /f /q || goto ERROR
del env\Python\python*.zip /f /q || goto ERROR

env\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --upgrade pip || goto ERROR
env\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: setuptools wheel || goto ERROR
env\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation --only-binary=:all: -r requirements_env.txt || goto ERROR
env\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation models\GraphCastOperationalIfs\weathernext.zip || goto ERROR
env\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation models\GraphCastOperationalGfs\graphcast.zip || goto ERROR
env\Python\python.exe -m pip install --no-warn-script-location --root-user-action ignore --no-build-isolation -e . || goto ERROR
env\Python\python.exe -m pip freeze --exclude-editable > frozen_requirements.txt || goto ERROR

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