#  Copyright (c) 2026 INFISYS INC

import numpy as np
import pandas as pd

from archive_forecast.log import log


@log
def resample(x, downscale, downscale_method):
	lat_min, lat_max = x.lat.values.min(), x.lat.values.max()
	lon_min, lon_max = x.lon.values.min(), x.lon.values.max()
	return x.interp({'lat': np.arange(lat_min, lat_max, downscale), 'lon': np.arange(lon_min, lon_max, downscale)}, downscale_method)


def clip(x, corners):
	return x.where(
		(x.lon >= min([c[0] % 360 for c in corners])) &
		(x.lon <= max([c[0] % 360 for c in corners])) &
		(x.lat >= min([c[1] for c in corners])) &
		(x.lat <= max([c[1] for c in corners])), drop=True)


@log
def zero_negative(x, fields):
	for f in fields:
		if f in x:
			x[f] = x[f].where(x[f] >= 0, 0)
	return x


@log
def update_datetime(x):
	return x.assign_coords(time=pd.to_datetime(x.coords['time']))


@log
def split(x):
	return x[[v for v in x.data_vars if 'level' not in x[v].dims]], x[[v for v in x.data_vars if 'level' in x[v].dims]]
