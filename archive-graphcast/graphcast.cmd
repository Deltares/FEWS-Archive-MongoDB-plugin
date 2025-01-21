::[config_path] [environment_install_base_path] [model_path]
::graphcast.cmd "C:\_GIT\TVA Graphcast\test\gfs_win_config.xml" "c:\_GIT\graphcast" "c:\Users\emroush.INFISYS\graphcast\models\GraphCastOperationalGfs"
@echo off
set miniconda = "Miniconda3-latest-Windows-x86_64.exe"
if not exist "%~2\%miniconda%" if exist "%~2" rmdir "%~2" /S /Q
if not exist "%~2" md "%~2"
if not exist "%~2\%miniconda%" powershell -Command "Invoke-WebRequest -Uri https://repo.anaconda.com/miniconda/%miniconda% -OutFile "%~2\%miniconda%"
if not exist "%~2\miniconda3" start /wait "" "%~2\%miniconda%" /AddToPath=1 /RegisterPython=1 /S /D=%~2\miniconda3
set conda_path=%~2\miniconda3;%~2\miniconda3\Library\mingw-w64\bin;%~2\miniconda3\Library\usr\bin;%~2\miniconda3\Library\bin;%~2\miniconda3\Scripts
echo %PATH% | findstr /C:"%conda_path%" >nul || set PATH=%conda_path%;%PATH%

set XLA_PYTHON_CLIENT_PREALLOCATE=false
set XLA_PYTHON_CLIENT_ALLOCATOR=platform

call conda config --add channels defaults
call conda config --set solver classic

call conda config --set ssl_verify false
pip config set global.trusted-host "pypi.python.org pypi.org files.pythonhosted.org"

call conda deactivate
call conda update conda -y
call conda update --all -y

if exist "%~2/miniconda3/envs/graphcast" call conda env remove -n graphcast -y
if exist "%~2/miniconda3/envs/graphcast" rmdir "%~2/miniconda3/envs/graphcast" /S /Q

call conda env create -n graphcast --file environment.yml -y
call conda activate graphcast

call conda clean --all -y
pip install --upgrade "%~3\graphcast.zip"

copy "%~3\.cdsapirc" "%UserProfile%"

python run_graphcast.py --config_path "%~1"
echo done