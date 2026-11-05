#  Copyright (c) 2026 INFISYS INC

from datetime import datetime

import numpy as np
import xarray as xr

from weathernext_3 import outputs


def _datasets():
	rng = np.random.default_rng(0)
	coords = {'time': np.array(['2026-11-03T07', '2026-11-03T08'], dtype='datetime64[ns]'), 'lat': [33.5, 33.6], 'lon': [271.0, 271.1, 271.2]}
	fields = ['temperature_2m', 'total_precipitation_1hr', 'imerg_tp_1hr']
	single_level = xr.Dataset({f: (('time', 'lat', 'lon'), rng.normal(0, 1, (2, 2, 3))) for f in fields}, coords=coords)
	percentiles = xr.Dataset({f: (('time', 'lat', 'lon', 'percentile'), rng.normal(0, 1, (2, 2, 3, 5))) for f in fields}, coords=coords | {'percentile': [10.0, 25.0, 50.0, 75.0, 90.0]})
	return single_level, percentiles


def test_write_creates_mean_and_percentile_files(tmp_path):
	single_level, percentiles = _datasets()
	outputs.write(single_level, percentiles, str(tmp_path / 'WeatherNext3Forecast.nc'), datetime(2026, 11, 3, 7))

	mean = xr.load_dataset(tmp_path / 'WeatherNext3Forecast.SingleLevel.202611030700.nc')
	spread = xr.load_dataset(tmp_path / 'WeatherNext3Forecast.Percentiles.202611030700.nc')

	assert set(mean.data_vars) == set(spread.data_vars) == {'temperature_2m', 'total_precipitation_1hr', 'imerg_tp_1hr'}
	assert spread['temperature_2m'].dims == ('time', 'lat', 'lon', 'percentile')
	assert spread['percentile'].attrs['units'] == 'percent'
	assert mean['temperature_2m'].attrs['abbreviation'] == 'TMP'


def test_write_zeroes_negative_precipitation_only(tmp_path):
	single_level, percentiles = _datasets()
	outputs.write(single_level, percentiles, str(tmp_path / 'WeatherNext3Forecast.nc'), datetime(2026, 11, 3, 7))

	for kind in ['SingleLevel', 'Percentiles']:
		x = xr.load_dataset(tmp_path / f'WeatherNext3Forecast.{kind}.202611030700.nc')
		assert (x['total_precipitation_1hr'].values >= 0).all()
		assert (x['imerg_tp_1hr'].values >= 0).all()
		assert (x['temperature_2m'].values < 0).any()
