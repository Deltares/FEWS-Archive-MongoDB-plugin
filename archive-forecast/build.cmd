:: Copyright (c) 2026 INFISYS INC

pushd archive_forecast && call .\build.cmd & popd
pushd graphcast_gfs && call .\build.cmd & popd
pushd weathernext_ifs && call .\build.cmd & popd
pushd weathernext_3 && call .\build.cmd & popd
