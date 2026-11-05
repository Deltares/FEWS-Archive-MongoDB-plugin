#  Copyright (c) 2026 INFISYS INC

import os

from archive_forecast.log import log


@log
def write(x, output_file, kind, t0):
	os.makedirs(os.path.dirname(output_file), exist_ok=True)
	name, extension = output_file.rsplit('.', 1)
	x.to_netcdf(f'{name}.{kind}.{t0:%Y%m%d%H%M}.{extension}', format='NETCDF4', encoding={v: {'zlib': True, 'complevel': 4} for v in x.data_vars})
