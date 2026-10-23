
:: Copyright (c) 2025 INFISYS INC

:: [config_path] [model_name]
:: graphcast_fews.cmd "c:\_GIT\FEWS-Archive\archive-graphcast\configurations\gfs_win_config.xml" "GraphCastOperationalGfs"

@echo off

cd %UserProfile%/graphcast

call .\graphcast.cmd "$1" "$2"