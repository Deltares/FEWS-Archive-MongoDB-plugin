#  Copyright (c) 2026 INFISYS INC

from datetime import datetime

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from archive_forecast.graphcast import common
from weathernext_ifs import forecast

_lat, _lon = np.array([0.0, 0.25]), np.array([0.0, 90.0, 180.0])


@pytest.fixture
def targets(monkeypatch):
	def solar(dates):
		return xr.Dataset({'toa_incident_solar_radiation': (('time', 'lat', 'lon'), np.stack([np.full((len(_lat), len(_lon)), pd.Timestamp(d).hour * 100.0) for d in dates]))})

	monkeypatch.setattr(forecast.solar, 'get_solar_radiation_cache', solar)
	inputs = xr.Dataset(coords={'lat': _lat, 'lon': _lon, 'level': [500], 'batch': [0]})
	return common.get_targets(inputs, datetime(2026, 11, 3, 0), 6, 2)


def test_get_forcings_builds_every_forcing_with_model_dimensions(targets):
	x = forecast._get_forcings(targets)
	assert set(x.data_vars) == {'year_progress_sin', 'year_progress_cos', 'day_progress_sin', 'day_progress_cos', 'toa_incident_solar_radiation'}
	for f in x.data_vars:
		assert x[f].dims == common.get_dims(f)


def test_get_forcings_progress_is_on_the_unit_circle(targets):
	x = forecast._get_forcings(targets)
	np.testing.assert_allclose(x['year_progress_sin'] ** 2 + x['year_progress_cos'] ** 2, 1.0)
	np.testing.assert_allclose(x['day_progress_sin'] ** 2 + x['day_progress_cos'] ** 2, 1.0)


def test_get_forcings_day_progress_follows_local_solar_time(targets):
	x = forecast._get_forcings(targets).isel(time=0, batch=0)
	np.testing.assert_allclose(x['day_progress_cos'].values, [1.0, 0.0, -1.0], atol=1e-6)


def test_get_forcings_uses_solar_cache_for_target_times(targets):
	x = forecast._get_forcings(targets)
	assert list(x['toa_incident_solar_radiation'].values[:, 0, 0, 0]) == [0.0, 600.0]
