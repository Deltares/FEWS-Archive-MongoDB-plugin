::[cpu|cuda] [environment_install_base_path]
::graphcast.cmd "cpu" "c:\_GIT\graphcast"
@echo off
set miniconda = "Miniconda3-latest-Windows-x86_64.exe"
if not exist "%~2\%miniconda%" if exist "%~2" rmdir "%~2" /S /Q
if not exist "%~2" md "%~2"
if not exist "%~2\%miniconda%" powershell -Command "Invoke-WebRequest -Uri https://repo.anaconda.com/%miniconda% -OutFile "%~2\%miniconda%"
if not exist "%~2\miniconda3" start /wait "" "%~2\%miniconda%" /AddToPath=1 /RegisterPython=1 /S /D=%~2\miniconda3
set conda_path=%~2\miniconda3;%~2\miniconda3\Library\mingw-w64\bin;%~2\miniconda3\Library\usr\bin;%~2\miniconda3\Library\bin;%~2\miniconda3\Scripts
echo %PATH% | findstr /C:"%conda_path%" >nul || set PATH=%conda_path%;%PATH%

call conda config --add channels defaults
call conda config --set solver classic

call conda config --set ssl_verify false
pip config set global.trusted-host "pypi.python.org pypi.org files.pythonhosted.org"

call conda deactivate
call conda update conda -y
call conda update --all -y

if exist "%~2/miniconda3/envs/graphcast" call conda env remove -n graphcast -y
if exist "%~2/miniconda3/envs/graphcast" rmdir "%~2/miniconda3/envs/graphcast" /S /Q

call conda create -n graphcast -y
call conda activate graphcast
call conda install python=3.12 -y

pip install "jax[cpu]" pysolar requests xmltodict netcdf4 zarr cdsapi
call conda install -c conda-forge cfgrib -y
powershell $e=(conda env export); $e[0..($e.Length-2)] > environment.yml

echo done