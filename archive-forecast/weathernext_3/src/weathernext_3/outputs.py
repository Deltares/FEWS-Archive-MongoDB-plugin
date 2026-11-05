#  Copyright (c) 2026 INFISYS INC

import os

from archive_forecast import metadata
from archive_forecast.log import log


@log
def write(single_level, percentiles, output_folder, output_file, t0):
	single_level = _zero_negative_precipitation(single_level)
	single_level = metadata.apply(single_level)
	percentiles = _zero_negative_precipitation(percentiles)
	percentiles = metadata.apply(percentiles)

	os.makedirs(output_folder, exist_ok=True)
	output_file_parts = output_file.split('.')

	single_level.to_netcdf(f"{'.'.join(output_file_parts[:-1])}.SingleLevel.{t0:%Y%m%d%H%M}.{output_file_parts[-1]}", format='NETCDF4', encoding={v: {'zlib': True, 'complevel': 4} for v in single_level.data_vars})
	percentiles.to_netcdf(f"{'.'.join(output_file_parts[:-1])}.Percentiles.{t0:%Y%m%d%H%M}.{output_file_parts[-1]}", format='NETCDF4', encoding={v: {'zlib': True, 'complevel': 4} for v in percentiles.data_vars})


@log
def _zero_negative_precipitation(x):
	for f in ['total_precipitation_1hr', 'imerg_tp_1hr', 'experimental_tp_1hr']:
		if f in x:
			x[f] = x[f].where(x[f] >= 0, 0)
	return x
