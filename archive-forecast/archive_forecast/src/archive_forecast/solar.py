#  Copyright (c) 2026 INFISYS INC

import os

import multiprocessing as mp
import xarray as xr
import numpy as np
import pandas as pd

from datetime import timedelta
from pysolar.solar import get_altitude
from pysolar.radiation import get_radiation_direct
from archive_forecast import cache

_home_path = cache.folder('archive_forecast')


def get_solar_radiation_cache(dates):
	cache = {}
	cache_dates = {pd.to_datetime(f.replace('solar_radiation_cache_', '').replace('.nc', '')): f for f in os.listdir(_home_path) if f.startswith('solar_radiation_cache_') and f.endswith('.nc')}
	for date in dates:
		if date in cache_dates:
			try:
				ds = xr.load_dataset(os.path.join(_home_path, cache_dates[date]))
			except Exception:
				_put_solar_radiation_cache(date)
				ds = xr.load_dataset(os.path.join(_home_path, cache_dates[date]))
		else:
			_put_solar_radiation_cache(date)
			ds = xr.load_dataset(os.path.join(_home_path, f'solar_radiation_cache_{date:%Y%m%d%H%M}.nc'))

		cache[date] = ds.assign_coords(time=pd.to_datetime(ds.coords['time'].values))

	[os.remove(os.path.join(_home_path, cache_dates[d])) for d in cache_dates if d < min(dates) - timedelta(days=7)]
	return xr.concat(cache.values(), dim='time')


def _get_solar_radiation(longitude, latitude, dt):
	watts_to_joules = 3600
	altitude_degrees = get_altitude(latitude, longitude, dt.tz_localize("UTC"))
	return watts_to_joules * get_radiation_direct(dt, altitude_degrees) if altitude_degrees > 0 else 0


def _get_solar_radiation_parallel(args):
	dt, lat = args
	return np.array([_get_solar_radiation(lon / 4, lat, dt) for lon in range(0, 360 * 4, 1)], dtype='float64')


def _put_solar_radiation_cache(date):
	latitudes = [lat / 4 for lat in range(-90 * 4, 90 * 4 + 1, 1)]
	with mp.Pool(mp.cpu_count()) as p:
		results = p.map(_get_solar_radiation_parallel, [(date, lat) for lat in latitudes])
	solar_radiation_cache = xr.Dataset({'toa_incident_solar_radiation': (('time', 'lat', 'lon'), np.stack(results)[np.newaxis])}, coords={'time': [date], 'lat': latitudes, 'lon': [lon / 4 for lon in range(0, 360 * 4, 1)]})
	solar_radiation_cache.to_netcdf(os.path.join(_home_path, f'solar_radiation_cache_{date:%Y%m%d%H%M}.nc'), format='NETCDF4', encoding={v: {'zlib': True, 'complevel': 4} for v in solar_radiation_cache.data_vars})
