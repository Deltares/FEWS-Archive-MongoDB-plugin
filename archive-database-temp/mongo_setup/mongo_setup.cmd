@echo off
setlocal
set operation=%1

whoami /groups | find "S-1-16-12288" >log.txt
set /p is_admin=<log.txt
if "%is_admin%"=="" (
	echo This script must be executed from an elevated shell >>log.txt
	goto error )
echo. >log.txt

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

if "%-install_dir%"=="" goto help
if "%-data_dir%"=="" goto help
if "%-log_dir%"=="" goto help
if "%-service_name%"=="" set -service_name="MongoDB"
if "%-port%"=="" set -port="27017"

echo Creating Directories...
if not exist %-install_dir% md %-install_dir%
if not exist %-data_dir% md %-data_dir%
if not exist %-log_dir% md %-log_dir%
if not exist .\install md .\install

sc query type=service state=all |find "%-service_name%" >NUL && sc query %-service_name% |find "RUNNING" >NUL
if %errorlevel%==0 (
	echo Stopping Service...
	sc stop %-service_name% >NUL )

sc query type=service state=all |find "%-service_name%" >NUL
if %errorlevel%==0 (
	echo Removing Service...
	sc delete %-service_name% >NUL )

echo Acquiring Binaries...
curl https://fastdl.mongodb.org/windows/mongodb-windows-x86_64-5.0.2.zip -o .\install\mongodb-windows-x86_64-5.0.2.zip 11>>log.txt 2>>&1 2>>&1
if %errorlevel%==1 goto error

echo Installing MongoDB...
tar -zxf .\install\mongodb-windows-x86_64-5.0.2.zip -C %-install_dir% 1>>log.txt 2>>&1
if %errorlevel%==1 goto error

xcopy /s /y /q %-install_dir%\mongodb-win32-x86_64-windows-5.0.2\* %-install_dir% 1>>log.txt 2>>&1
if %errorlevel%==1 goto error

rd /s /q %-install_dir%\mongodb-win32-x86_64-windows-5.0.2 >NUL

del %-install_dir%\bin\Install-Compass.ps1 >NUL

::%-install_dir%\bin\vcredist_x64.exe

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
%-install_dir%\bin\mongod.exe --install --config %-install_dir%\bin\mongod.cfg --serviceName %-service_name% --serviceDisplayName "MongoDB Server (%-service_name%)" 1>>log.txt 2>>&1
if %errorlevel%==1 goto error

echo Starting Service...
sc start %-service_name% 1>>log.txt 2>>&1
if %errorlevel%==1 goto error

ping localhost -n 10 >NUL

goto summary

:remove

if "%-install_dir%"=="" goto help
if "%-service_name%"=="" goto help

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

goto summary

:help

echo.
echo HELP:
echo mongo_setup.cmd [command] -[options]
echo.
echo ========================================================
echo [command]: install
echo ========================================================
echo [options]:
echo   -install_dir: where to place the Mongo DB binaries.
echo   -data_dir: where to place the Mongo DB data files.
echo   -log_dir: where to place the Mongo DB log files.
echo   -service_name: the name of the service running MongoDB. Default=MongoDB.
echo   -port: the port to use for accessing Mongo DB. Default=27217.
echo.
echo EXAMPLE:
echo mongo_setup.cmd install -install_dir c:\test\install -data_dir c:\test\data -log_dir c:\test\log -service_name TestFEWSArchiveMongoDB -port 27019
echo.
echo.
echo ======================================================== 
echo [commnad]: remove 
echo ========================================================
echo [options]:
echo   -install_dir: where the Mongo DB binaries were installed.
echo   -service_name: the name of the service running MongoDB.
echo.
echo EXAMPLE:
echo mongo_setup.cmd remove -install_dir c:\test\install -service_name TestFEWSArchiveMongoDB
echo.
echo.

goto end

:summary

echo Complete!
goto end

:error

echo [%*] 
type log.txt
goto end

:end