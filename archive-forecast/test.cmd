:: Copyright (c) 2026 INFISYS INC

pushd archive_forecast && build\Python\python.exe -m pytest & popd
pushd graphcast_gfs && build\Python\python.exe -m pytest & popd
pushd weathernext_ifs && build\Python\python.exe -m pytest & popd
pushd weathernext_3 && build\Python\python.exe -m pytest & popd
