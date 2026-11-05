#  Copyright (c) 2026 INFISYS INC

import numpy as np
import pandas as pd
import xarray as xr

from archive_forecast.graphcast import utils


def _grid():
	lat, lon = np.arange(-90.0, 90.25, 0.25), np.arange(0.0, 360.0, 0.25)
	return xr.Dataset({'t': (('lat', 'lon'), np.zeros((len(lat), len(lon))))}, coords={'lat': lat, 'lon': lon})


def test_clip_converts_western_longitudes_and_keeps_bounding_box():
	x = utils.clip(_grid(), [[-89.47, 38.14], [-81.05, 33.40]])
	assert (x.lat.min(), x.lat.max()) == (33.5, 38.0)
	assert (x.lon.min(), x.lon.max()) == (270.75, 278.75)


def test_clip_corner_order_does_not_matter():
	a = utils.clip(_grid(), [[-89.47, 38.14], [-81.05, 33.40]])
	b = utils.clip(_grid(), [[-81.05, 33.40], [-89.47, 38.14]])
	assert a.identical(b)


def test_resample_uses_requested_grid_spacing():
	lat, lon = np.arange(30.0, 32.25, 0.25), np.arange(270.0, 272.25, 0.25)
	x = xr.Dataset({'t': (('lat', 'lon'), np.add.outer(lat, lon))}, coords={'lat': lat, 'lon': lon})
	y = utils.resample(x, 0.0625, 'linear')
	np.testing.assert_allclose(np.diff(y.lat.values), 0.0625)
	np.testing.assert_allclose(np.diff(y.lon.values), 0.0625)
	np.testing.assert_allclose(y['t'].values, np.add.outer(y.lat.values, y.lon.values))


def test_update_datetime_converts_time_coordinate():
	x = xr.Dataset({'t': ('time', [1.0])}, coords={'time': np.array(['2026-11-03T12'], dtype='datetime64[us]')})
	y = utils.update_datetime(x)
	assert pd.api.types.is_datetime64_any_dtype(y['time'])
	assert pd.Timestamp(y['time'].values[0]) == pd.Timestamp('2026-11-03 12:00')


def test_strip_drops_attributes_and_extra_coordinates():
	x = xr.Dataset({'t': (('lat',), [1.0, 2.0], {'GRIB_units': 'K'})}, coords={'lat': [0.0, 1.0], 'step': 0}, attrs={'GRIB_edition': 2})
	y = utils.strip(x)
	assert y.attrs == {}
	assert y['t'].attrs == {}
	assert 'step' not in y.coords
	np.testing.assert_array_equal(y['t'].values, [1.0, 2.0])


def test_split_separates_single_and_pressure_level_fields():
	x = xr.Dataset({'surface': (('lat',), [1.0]), 'upper': (('level', 'lat'), [[1.0]])}, coords={'lat': [0.0], 'level': [500]})
	single_level, pressure_levels = utils.split(x)
	assert list(single_level.data_vars) == ['surface']
	assert list(pressure_levels.data_vars) == ['upper']
