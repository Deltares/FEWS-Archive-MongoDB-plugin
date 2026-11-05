:: Copyright (c) 2026 INFISYS INC
@setlocal

pushd archive_forecast || goto ERROR
call .\build.cmd || ( popd & goto ERROR )
popd

pushd graphcast_gfs || goto ERROR
call .\build.cmd || ( popd & goto ERROR )
popd

pushd weathernext_ifs || goto ERROR
call .\build.cmd || ( popd & goto ERROR )
popd

pushd weathernext_3 || goto ERROR
call .\build.cmd || ( popd & goto ERROR )
popd

@echo SUCCESS
@goto END

:ERROR
@echo ERROR
@endlocal
@exit /b 1

:END
@endlocal
