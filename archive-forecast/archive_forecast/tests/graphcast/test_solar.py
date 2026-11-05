#  Copyright (c) 2026 INFISYS INC

import os

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from archive_forecast.graphcast import solar


@pytest.fixture
def solar_cache(tmp_path, monkeypatch):
	calls = []

	def put(date):
		calls.append(date)
		xr.Dataset({'toa_incident_solar_radiation': (('time', 'lat', 'lon'), np.full((1, 2, 2), float(date.hour)))}, coords={'time': [date], 'lat': [0.0, 0.25], 'lon': [0.0, 0.25]}).to_netcdf(tmp_path / f'solar_radiation_cache_{date:%Y%m%d%H%M}.nc')

	monkeypatch.setattr(solar, '_home_path', str(tmp_path))
	monkeypatch.setattr(solar, '_put_solar_radiation_cache', put)
	return tmp_path, calls


def test_get_solar_radiation_cache_computes_missing_dates_then_reuses_them(solar_cache):
	_, calls = solar_cache
	dates = [pd.Timestamp('2026-11-03 06:00'), pd.Timestamp('2026-11-03 12:00')]

	x = solar.get_solar_radiation_cache(dates)
	y = solar.get_solar_radiation_cache(dates)

	assert calls == dates
	assert x.identical(y)
	assert list(x['toa_incident_solar_radiation'].values[:, 0, 0]) == [6.0, 12.0]


def test_get_solar_radiation_cache_rebuilds_unreadable_file(solar_cache):
	path, calls = solar_cache
	date = pd.Timestamp('2026-11-03 06:00')
	(path / 'solar_radiation_cache_202611030600.nc').write_text('corrupt')

	x = solar.get_solar_radiation_cache([date])

	assert calls == [date]
	assert x['toa_incident_solar_radiation'].values[0, 0, 0] == 6.0


def test_get_solar_radiation_cache_removes_files_a_week_older_than_the_forecast(solar_cache):
	path, _ = solar_cache
	solar.get_solar_radiation_cache([pd.Timestamp('2026-10-20 00:00')])

	solar.get_solar_radiation_cache([pd.Timestamp('2026-11-03 00:00')])

	assert sorted(os.listdir(path)) == ['solar_radiation_cache_202611030000.nc']


def test_solar_radiation_is_zero_at_night_and_positive_at_noon():
	assert solar._get_solar_radiation(0.0, 0.0, pd.Timestamp('2026-11-03 00:00')) == 0
	assert solar._get_solar_radiation(0.0, 0.0, pd.Timestamp('2026-11-03 12:00')) > 0


def test_solar_radiation_row_covers_every_quarter_degree_of_longitude():
	row = solar._get_solar_radiation_parallel((pd.Timestamp('2026-11-03 12:00'), 0.0))
	assert row.shape == (1440,)
	assert row[0] > 0
	assert row[720] == 0
