#  Copyright (c) 2026 INFISYS INC

from datetime import datetime

import numpy as np
import xarray as xr

from graphcast_gfs import outputs


def _forecast():
	rng = np.random.default_rng(0)
	time = np.array(['2026-11-03T12', '2026-11-03T18'], dtype='datetime64[us]')
	lat, lon, level = np.arange(33.0, 35.25, 0.25), np.arange(270.0, 272.25, 0.25), np.array([1000, 500, 50])
	return xr.Dataset({
		'2m_temperature': (('time', 'lat', 'lon'), rng.normal(280, 5, (2, len(lat), len(lon)))),
		'total_precipitation_6hr': (('time', 'lat', 'lon'), rng.normal(0, 0.001, (2, len(lat), len(lon)))),
		'temperature': (('time', 'level', 'lat', 'lon'), rng.normal(260, 5, (2, 3, len(lat), len(lon))))},
		coords={'time': time, 'lat': lat, 'lon': lon, 'level': level})


def test_write_creates_single_level_and_pressure_level_files(tmp_path):
	outputs.write(_forecast(), str(tmp_path / 'GraphCastForecast.nc'), 0.0625, 'cubic', datetime(2026, 11, 3, 12))

	single_level = xr.load_dataset(tmp_path / 'GraphCastForecast.SingleLevel.202611031200.nc')
	pressure_levels = xr.load_dataset(tmp_path / 'GraphCastForecast.PressureLevels.202611031200.nc')

	assert set(single_level.data_vars) == {'2m_temperature', 'total_precipitation_6hr'}
	assert set(pressure_levels.data_vars) == {'temperature'}
	np.testing.assert_allclose(np.diff(single_level['lat'].values), 0.0625)
	assert (single_level['total_precipitation_6hr'].values >= 0).all()
	assert single_level['2m_temperature'].attrs['units'] == 'K'
	assert pressure_levels['level'].attrs['units'] == 'hPa'
	assert single_level.attrs['Conventions'] == 'CF-1.6'
