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

rmdir archive_forecast.egg-info /s /q
rmdir build\bdist.win-amd64 /s /q
rmdir build\lib /s /q

@echo SUCCESS
@goto END

:ERROR
@echo ERROR
@goto END

:END
@endlocal
