#  Copyright (c) 2026 INFISYS INC

from archive_forecast import metadata, netcdf, utils
from archive_forecast.graphcast import utils as graphcast_utils
from archive_forecast.log import log

_precipitation = ['total_precipitation_6hr']


@log
def write(model_forecast, output_file, downscale, downscale_method, t0):
	model_forecast = graphcast_utils.resample(model_forecast, downscale, downscale_method)
	model_forecast = utils.zero_negative(model_forecast, _precipitation)
	model_forecast = metadata.apply(model_forecast)
	model_forecast = graphcast_utils.update_datetime(model_forecast)
	single_level, pressure_levels = graphcast_utils.split(model_forecast)

	netcdf.write(single_level, output_file, 'SingleLevel', t0)
	netcdf.write(pressure_levels, output_file, 'PressureLevels', t0)
