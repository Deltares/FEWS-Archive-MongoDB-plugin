#  Copyright (c) 2026 INFISYS INC

import json

from datetime import datetime

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from graphcast_gfs import inputs

_latitude, _longitude, _levels = np.array([1.0, 0.5, 0.0]), np.array([0.0, 0.5]), np.array([1000, 500, 50])


def _raw(observed):
	time = [pd.Timestamp(observed).as_unit('ns')]
	shape = (1, len(_latitude), len(_longitude))
	single_level = xr.Dataset({f: (('time', 'latitude', 'longitude'), np.full(shape, observed.hour + i, dtype='float64'), {'GRIB_units': 'x'}) for i, f in enumerate(['orog', 'prmsl', 'lsm', 't2m', 'u10', 'v10'])}, coords={'time': time, 'latitude': _latitude, 'longitude': _longitude})
	pressure_levels = xr.Dataset({f: (('time', 'isobaricInhPa', 'latitude', 'longitude'), np.full((1, len(_levels)) + shape[1:], observed.hour + i, dtype='float64')) for i, f in enumerate(['gh', 't', 'q', 'w', 'u', 'v'])}, coords={'time': time, 'isobaricInhPa': _levels, 'latitude': _latitude, 'longitude': _longitude})
	return single_level, pressure_levels


@pytest.fixture
def observed(monkeypatch):
	calls = []
	monkeypatch.setattr(inputs.cache, 'clean', lambda path: None)
	monkeypatch.setattr(inputs, '_get_observed', lambda o: calls.append(o) or _raw(o))
	return calls


def test_read_fetches_each_observed_time_once(observed):
	inputs.read(datetime(2026, 11, 3, 12), 6, 2)
	assert observed == [datetime(2026, 11, 3, 0), datetime(2026, 11, 3, 6)]


def test_read_renames_sorts_and_strips(observed):
	single_level, pressure_levels = inputs.read(datetime(2026, 11, 3, 12), 6, 2)

	assert set(single_level.data_vars) == {'geopotential_at_surface', 'mean_sea_level_pressure', 'land_sea_mask', '2m_temperature', '10m_u_component_of_wind', '10m_v_component_of_wind'}
	assert set(pressure_levels.data_vars) == {'geopotential', 'temperature', 'specific_humidity', 'vertical_velocity', 'u_component_of_wind', 'v_component_of_wind'}
	assert list(single_level['lat'].values) == [0.0, 0.5, 1.0]
	assert list(pressure_levels['level'].values) == [50, 500, 1000]
	assert single_level.sizes['time'] == 2
	assert single_level['2m_temperature'].attrs == {}
	assert list(single_level['geopotential_at_surface'].values[:, 0, 0]) == [0.0, 6.0]


class _Response:
	def __init__(self, status_code, content=b'grib'):
		self.status_code, self.content, self.text = status_code, content, 'error'


class _Session:
	def __init__(self, response):
		self.response, self.urls = response, []

	def __enter__(self):
		return self

	def __exit__(self, *args):
		pass

	def get(self, url, verify):
		self.urls.append(url)
		return self.response


def test_put_cache_requests_filter_url_and_writes_file(tmp_path, monkeypatch):
	session = _Session(_Response(200))
	monkeypatch.setattr(inputs.web, 'session', lambda: session)
	filename = tmp_path / 'data.grib2'

	inputs._put_cache('https://nomads/filter?file=x', ['&var_TMP=on', '&lev_2_m_above_ground=on'], str(filename))

	assert session.urls == ['https://nomads/filter?file=x&var_TMP=on&lev_2_m_above_ground=on']
	assert filename.read_bytes() == b'grib'


def test_put_cache_raises_on_http_error(tmp_path, monkeypatch):
	monkeypatch.setattr(inputs.web, 'session', lambda: _Session(_Response(404)))
	with pytest.raises(ImportError):
		inputs._put_cache('https://nomads/filter', [], str(tmp_path / 'data.grib2'))
	assert not (tmp_path / 'data.grib2').exists()


def _grib(request):
	coords = {'latitude': _latitude, 'longitude': _longitude, 'step': 0}
	shape = (len(_latitude), len(_longitude))
	if any('_mb=' in r for r in request):
		return xr.Dataset({f: (('isobaricInhPa', 'latitude', 'longitude'), np.ones((len(_levels),) + shape)) for f in ['gh', 't', 'q', 'w', 'u', 'v']}, coords=coords | {'isobaricInhPa': _levels})
	names = {'&var_HGT=on': ['orog'], '&var_LAND=on': ['lsm', 'prmsl'], '&var_TMP=on': ['t2m'], '&var_UGRD=on': ['u10', 'v10']}[request[0]]
	return xr.Dataset({f: (('latitude', 'longitude'), np.ones(shape)) for f in names}, coords=coords)


def test_get_observed_requests_fields_and_converts_geopotential(monkeypatch):
	queries = []
	monkeypatch.setattr(inputs, '_query', lambda url, request: queries.append((url, request)) or _grib(request))

	single_level, pressure_levels = inputs._get_observed(datetime(2026, 11, 3, 6))

	assert {url for url, _ in queries} == {'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?dir=/gfs.20261103/06/atmos&file=gfs.t06z.pgrb2.0p25.f000'}
	assert len(queries) == 5
	assert '&lev_50_mb=on' in queries[-1][1]
	assert set(single_level.data_vars) == {'orog', 'lsm', 'prmsl', 't2m', 'u10', 'v10'}
	assert set(pressure_levels.data_vars) == {'gh', 't', 'q', 'w', 'u', 'v'}
	assert single_level['orog'].values[0, 0, 0] == pytest.approx(9.80665)
	assert pressure_levels['gh'].values[0, 0, 0, 0] == pytest.approx(9.80665)
	assert single_level['t2m'].values[0, 0, 0] == 1.0
	assert 'step' not in single_level.coords
	assert list(single_level['time'].values) == [np.datetime64('2026-11-03T06:00', 'ns')]


def test_query_caches_grib_by_url_and_request(monkeypatch):
	calls = []
	monkeypatch.setattr(inputs.cache, 'get', lambda path, key, extension, load, put: calls.append((path, json.loads(key), extension, load)))

	inputs._query('https://nomads/filter', ['&var_TMP=on'])

	path, key, extension, load = calls[0]
	assert path == inputs._home_path
	assert key == {'https://nomads/filter': ['&var_TMP=on']}
	assert extension == 'grib2'
	assert load is xr.load_dataset
