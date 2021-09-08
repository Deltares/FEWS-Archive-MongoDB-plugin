@echo off
setlocal
set operation=%1

:variables
if not "%2"=="" (
	set %2=%3
    shift
	shift
	goto variables
)
if "%operation%"=="install" goto %operation%
if "%operation%"=="remove" goto %operation%
goto help

:install

if "%-install_dir%"=="" goto help_install
if "%-data_dir%"=="" goto help_install
if "%-log_dir%"=="" goto help_install
if "%-service_name%"=="" goto help_install
if "%-port%"=="" goto help_install

echo Creating Directories...
if not exist %-install_dir% md %-install_dir%
if not exist %-data_dir% md %-data_dir%
if not exist %-log_dir% md %-log_dir%

sc query type=service state=all |find "%-service_name%" >NUL && sc query %-service_name% |find "RUNNING" >NUL
if %errorlevel%==0 (
	echo Stopping Service...
	sc stop %-service_name% >NUL )

sc query type=service state=all |find "%-service_name%" >NUL
if %errorlevel%==0 (
	echo Removing Service...
	sc delete %-service_name% >NUL )

echo Installing MongoDB...
.\7z\7za.exe x -y -o%-install_dir% .\mongodb\mongodb-windows-x86_64-4.4.3.zip >NUL
xcopy /s /y /q %-install_dir%\mongodb-win32-x86_64-windows-4.4.3\* %-install_dir% >NUL
rd /s /q %-install_dir%\mongodb-win32-x86_64-windows-4.4.3 >NUL
del %-install_dir%\bin\Install-Compass.ps1 >NUL

echo Writing Configuration...
(
echo storage:
echo   dbPath: %-data_dir%
echo   journal:
echo     enabled: true
echo systemLog:
echo   destination: "file"
echo   path: %-log_dir%\mongod.log
echo net:
echo   bindIp: 0.0.0.0
echo   port: %-port%
echo security:
echo   authorization: "disabled"
)>%-install_dir%\bin\mongod.cfg

echo Installing Service...
%-install_dir%\bin\mongod.exe --install --config %-install_dir%\bin\mongod.cfg --serviceName %-service_name% --serviceDisplayName "MongoDB Server (%-service_name%)" >NUL

echo Starting Service...
sc start %-service_name% >NUL

ping localhost -n 10 >NUL

goto end

:remove

if "%-install_dir%"=="" goto help_remove
if "%-service_name%"=="" goto help_remove

sc query type=service state=all |find "%-service_name%" >NUL && sc query %-service_name% |find "RUNNING" >NUL
if %errorlevel%==0 (
	echo Stopping Service...
	sc stop %-service_name% >NUL )

sc query type=service state=all |find "%-service_name%" >NUL
if %errorlevel%==0 (
	echo Removing Service...
	sc delete %-service_name% >NUL )

if exist %-install_dir% (
	echo Removing MongoDB...
	rd /s /q %-install_dir% >NUL )

goto end

:help

echo mongo_setup.cmd install -install_dir install_dir -data_dir data_dir -log_dir log_dir -service_name service_name -port port
echo mongo_setup.cmd install -install_dir f:\test\install -data_dir f:\test\data -log_dir f:\test\log -service_name TestFEWSArchiveMongoDB -port 27019

echo mongo_setup.cmd remove -install_dir install_dir -service_name service_name
echo mongo_setup.cmd remove -install_dir f:\test\install -service_name TestFEWSArchiveMongoDB

goto end

:help_install

echo mongo_setup.cmd install -install_dir install_dir -data_dir data_dir -log_dir log_dir -service_name service_name -port port
echo -install_dir="%-install_dir%"
echo -data_dir="%-data_dir%"
echo -log_dir="%-log_dir%"
echo -service_name="%-service_name%"
echo -port="%-port%"

goto end

:help_remove

echo mongo_setup.cmd remove -install_dir install_dir -service_name service_name
echo -install_dir="%-install_dir%"
echo -service_name="%-service_name%"

goto end

:end

echo [%*] 
echo Complete!