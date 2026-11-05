#  Copyright (c) 2026 INFISYS INC

from datetime import datetime

import numpy as np
import pandas as pd
import xarray as xr

from archive_forecast.graphcast import common

_lat, _lon, _level = np.arange(30.0, 31.0, 0.25), np.arange(270.0, 271.5, 0.25), np.array([50, 500, 1000])
_times = pd.to_datetime(['2026-11-03 00:00', '2026-11-03 06:00']).values.astype('datetime64[ns]')


def _observed():
	single_level = xr.Dataset({f: (('time', 'lat', 'lon'), np.random.default_rng(0).normal(size=(2, len(_lat), len(_lon)))) for f in ['geopotential_at_surface', 'land_sea_mask', 'mean_sea_level_pressure', '2m_temperature', '10m_u_component_of_wind', '10m_v_component_of_wind']}, coords={'time': _times, 'lat': _lat, 'lon': _lon})
	pressure_levels = xr.Dataset({f: (('time', 'lat', 'lon', 'level'), np.random.default_rng(1).normal(size=(2, len(_lat), len(_lon), 3))) for f in ['geopotential', 'temperature', 'specific_humidity', 'vertical_velocity', 'u_component_of_wind', 'v_component_of_wind']}, coords={'time': _times, 'lat': _lat, 'lon': _lon, 'level': _level})
	return single_level, pressure_levels


def test_get_inputs_shapes_fields_for_the_model():
	single_level, pressure_levels = _observed()
	x = common.get_inputs(single_level, pressure_levels)
	assert x['geopotential_at_surface'].dims == ('lat', 'lon')
	assert x['2m_temperature'].dims == ('time', 'lat', 'lon', 'batch')
	assert x['temperature'].dims == ('time', 'lat', 'lon', 'batch', 'level')
	assert x.sizes['batch'] == 1
	np.testing.assert_array_equal(x['geopotential_at_surface'].values, single_level['geopotential_at_surface'].isel(time=0).values)
	np.testing.assert_array_equal(x['temperature'].values[..., 0, :], pressure_levels['temperature'].values)


def test_get_dims_orders_dimensions():
	assert common.get_dims('temperature') == ('time', 'lat', 'lon', 'level', 'batch')
	assert common.get_dims('2m_temperature') == ('time', 'lat', 'lon', 'batch')
	assert common.get_dims('land_sea_mask') == ('lat', 'lon')
	assert common.get_dims('year_progress_sin') == ('time', 'batch')


def test_get_targets_builds_empty_template_for_each_step():
	single_level, pressure_levels = _observed()
	x = common.get_targets(common.get_inputs(single_level, pressure_levels), datetime(2026, 11, 3, 12), 6, 4)
	assert list(x['time'].values) == list(np.array(['2026-11-03T12', '2026-11-03T18', '2026-11-04T00', '2026-11-04T06'], dtype='datetime64[us]'))
	assert set(x.data_vars) == set(common._prediction_fields)
	assert x['temperature'].dtype == np.float32
	assert x['temperature'].sizes == {'time': 4, 'lat': len(_lat), 'lon': len(_lon), 'level': 3, 'batch': 1}
	assert np.isnan(x['temperature'].values).all()


def test_trim_keeps_prediction_fields_sorts_levels_descending_and_drops_batch():
	single_level, pressure_levels = _observed()
	x = common.get_targets(common.get_inputs(single_level, pressure_levels), datetime(2026, 11, 3, 12), 6, 1)
	x['extra'] = x['2m_temperature']
	y = common.trim(x)
	assert 'extra' not in y
	assert 'batch' not in y.dims
	assert list(y['level'].values) == [1000, 500, 50]
