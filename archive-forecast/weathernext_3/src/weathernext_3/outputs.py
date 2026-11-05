#  Copyright (c) 2026 INFISYS INC

from archive_forecast import metadata, netcdf, utils
from archive_forecast.log import log

_precipitation = ['total_precipitation_1hr', 'imerg_tp_1hr', 'experimental_tp_1hr']


@log
def write(single_level, percentiles, output_file, t0):
	single_level = utils.zero_negative(single_level, _precipitation)
	single_level = metadata.apply(single_level)

	percentiles = utils.zero_negative(percentiles, _precipitation)
	percentiles = metadata.apply(percentiles)

	netcdf.write(single_level, output_file, 'SingleLevel', t0)
	netcdf.write(percentiles, output_file, 'Percentiles', t0)
