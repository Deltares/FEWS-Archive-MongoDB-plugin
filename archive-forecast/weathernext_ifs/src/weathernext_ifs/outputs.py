#  Copyright (c) 2026 INFISYS INC

import os

import numpy as np
import pandas as pd

from archive_forecast import metadata
from archive_forecast.log import log


@log
def write(model_forecast, output_folder, output_file, downscale, downscale_method, t0):
	model_forecast = _resample(model_forecast, downscale, downscale_method)
	model_forecast = _zero_negative_precipitation(model_forecast)
	model_forecast = metadata.apply(model_forecast)
	model_forecast = _update_datetime(model_forecast)
	single_level, pressure_levels = _split(model_forecast)

	os.makedirs(output_folder, exist_ok=True)
	output_file_parts = output_file.split('.')

	single_level.to_netcdf(f"{'.'.join(output_file_parts[:-1])}.SingleLevel.{t0:%Y%m%d%H%M}.{output_file_parts[-1]}", format='NETCDF4', encoding={v: {'zlib': True, 'complevel': 4} for v in single_level.data_vars})
	pressure_levels.to_netcdf(f"{'.'.join(output_file_parts[:-1])}.PressureLevels.{t0:%Y%m%d%H%M}.{output_file_parts[-1]}", format='NETCDF4', encoding={v: {'zlib': True, 'complevel': 4} for v in pressure_levels.data_vars})


@log
def _update_datetime(x):
	return x.assign_coords(time=pd.to_datetime(x.coords['time']))


@log
def _zero_negative_precipitation(x):
	x['total_precipitation_6hr'] = x['total_precipitation_6hr'].where(x['total_precipitation_6hr'] >= 0, 0)
	return x


@log
def _resample(x, downscale, downscale_method):
	lat_min, lat_max = x.lat.values.min(), x.lat.values.max()
	lon_min, lon_max = x.lon.values.min(), x.lon.values.max()
	return x.interp({'lat': np.arange(lat_min, lat_max, downscale), 'lon': np.arange(lon_min, lon_max, downscale)}, downscale_method)


@log
def _split(x):
	return x[[v for v in x.data_vars if 'level' not in x[v].dims]], x[[v for v in x.data_vars if 'level' in x[v].dims]]
