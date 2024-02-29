
@echo off
setlocal

:operation
set /P operation="Would you like to [i]nstall or [r]emove mongodb: "

if "%operation%"=="i" goto install
if "%operation%"=="r" goto remove
goto operation

echo "Leave blank for default"


:install
set "install_dir=f:\FEWSArchiveMongoDB\install"
set /P install_dir="Install directory [f:\FEWSArchiveMongoDB\install]: "

set "data_dir=f:\FEWSArchiveMongoDB\data"
set /P data_dir="Data directory [f:\FEWSArchiveMongoDB\data]: "

set "log_dir=f:\FEWSArchiveMongoDB\log"
set /P log_dir="Log directory [f:\FEWSArchiveMongoDB\log]: "

set "service_name=MongoDB"
set /P service_name="Mongo Service Name [MongoDB]: "

set "port=27017"
set /P port="Port Number [27017]: "

mongo_setup.cmd install -install_dir %install_dir% -data_dir %data_dir% -log_dir %log_dir% -service_name %service_name% -port %port%


goto end


:remove
set "install_dir=f:\FEWSArchiveMongoDB\install"
set /P install_dir="Install directory [f:\FEWSArchiveMongoDB\install]: "

set "service_name=MongoDB"
set /P service_name="Mongo Service Name [MongoDB]: "

mongo_setup.cmd remove -install_dir %install_dir% -service_name %service_name%
goto end


:end