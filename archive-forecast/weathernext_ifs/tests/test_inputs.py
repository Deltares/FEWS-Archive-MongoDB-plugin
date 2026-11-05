#  Copyright (c) 2026 INFISYS INC

import json

from datetime import datetime

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from weathernext_ifs import inputs

_latitude, _longitude, _levels = np.array([1.0, 0.5, 0.0]), np.array([0.0, 0.5]), np.array([1000, 500, 50])


def _raw(observed):
	time = [pd.Timestamp(observed).as_unit('ns')]
	shape = (1, len(_latitude), len(_longitude))
	single_level = xr.Dataset({f: (('time', 'latitude', 'longitude'), np.full(shape, observed.hour + i, dtype='float64'), {'GRIB_units': 'x'}) for i, f in enumerate(['z', 'msl', 'lsm', 't2m', 'u10', 'v10'])}, coords={'time': time, 'latitude': _latitude, 'longitude': _longitude})
	pressure_levels = xr.Dataset({f: (('time', 'isobaricInhPa', 'latitude', 'longitude'), np.full((1, len(_levels)) + shape[1:], observed.hour + i, dtype='float64')) for i, f in enumerate(['u', 'v', 'z', 'q', 't', 'w'])}, coords={'time': time, 'isobaricInhPa': _levels, 'latitude': _latitude, 'longitude': _longitude})
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


def test_read_index_keys_surface_and_pressure_level_entries(tmp_path):
	entries = [
		{'levtype': 'sfc', 'param': 'msl', '_offset': 0, '_length': 10},
		{'levtype': 'pl', 'param': 't', 'levelist': '500', '_offset': 10, '_length': 20}]
	path = tmp_path / 'file.index'
	path.write_text('\n'.join(json.dumps(e) for e in entries))

	index = inputs._read_index(str(path))

	assert index[('sfc', 'msl')]['_offset'] == 0
	assert index[('pl', 't', '500')]['_length'] == 20


class _Response:
	def __init__(self, status_code, content=b'grib', text='index'):
		self.status_code, self.content, self.text = status_code, content, text


class _Session:
	def __init__(self, response):
		self.response, self.requests = response, []

	def __enter__(self):
		return self

	def __exit__(self, *args):
		pass

	def get(self, url, headers=None, verify=True):
		self.requests.append((url, headers))
		return self.response


def test_put_data_cache_requests_byte_range_and_writes_file(tmp_path, monkeypatch):
	session = _Session(_Response(206))
	monkeypatch.setattr(inputs.web, 'session', lambda: session)
	filename = tmp_path / 'data.grib2'

	inputs._put_data_cache('https://mirror/file.grib2', {'_offset': 100, '_length': 50}, str(filename))

	assert session.requests == [('https://mirror/file.grib2', {'Range': 'bytes=100-149'})]
	assert filename.read_bytes() == b'grib'


def test_put_data_cache_requires_partial_content(tmp_path, monkeypatch):
	monkeypatch.setattr(inputs.web, 'session', lambda: _Session(_Response(200)))
	with pytest.raises(ImportError):
		inputs._put_data_cache('https://mirror/file.grib2', {'_offset': 0, '_length': 1}, str(tmp_path / 'data.grib2'))


def test_put_index_cache_writes_text(tmp_path, monkeypatch):
	monkeypatch.setattr(inputs.web, 'session', lambda: _Session(_Response(200, text='{"param": "msl"}')))
	filename = tmp_path / 'file.index'

	inputs._put_index_cache('https://mirror/file.index', str(filename))

	assert filename.read_text() == '{"param": "msl"}'


def test_get_index_requests_index_file_for_grib_url(monkeypatch):
	urls = []
	monkeypatch.setattr(inputs.cache, 'get', lambda path, key, extension, load, put: urls.append(json.loads(key)) or {})
	inputs._get_index('https://mirror/20261103000000-0h-oper-fc.grib2')
	assert urls == [{'https://mirror/20261103000000-0h-oper-fc.index': None}]


def _grib(entry):
	coords = {'latitude': _latitude, 'longitude': np.array([-90.0, 90.0]), 'step': 0}
	shape = (len(_latitude), 2)
	if entry['levtype'] == 'pl':
		return xr.Dataset({entry['param']: (('latitude', 'longitude'), np.full(shape, float(entry['levelist'])))}, coords=coords | {'isobaricInhPa': float(entry['levelist'])})
	return xr.Dataset({{'2t': 't2m', '10u': 'u10', '10v': 'v10'}.get(entry['param'], entry['param']): (('latitude', 'longitude'), np.ones(shape))}, coords=coords)


class _Index(dict):
	def __missing__(self, key):
		return {'levtype': key[0], 'param': key[1]} | ({'levelist': key[2]} if len(key) > 2 else {})


def test_get_observed_requests_each_field_from_the_index(monkeypatch):
	urls, queries = [], []
	monkeypatch.setattr(inputs, '_get_index', lambda url: urls.append(url) or _Index())
	monkeypatch.setattr(inputs, '_query', lambda url, entry: queries.append(entry) or _grib(entry))

	single_level, pressure_levels = inputs._get_observed(datetime(2026, 11, 3, 6))

	assert urls == ['https://storage.googleapis.com/ecmwf-open-data/20261103/06z/ifs/0p25/oper/20261103060000-0h-oper-fc.grib2']
	assert len(queries) == 6 + 6 * 13
	assert set(single_level.data_vars) == {'z', 'lsm', 'msl', 't2m', 'u10', 'v10'}
	assert set(pressure_levels.data_vars) == {'u', 'v', 'z', 'q', 't', 'w'}
	assert list(pressure_levels['isobaricInhPa'].values) == [1000, 925, 850, 700, 600, 500, 400, 300, 250, 200, 150, 100, 50]
	assert list(pressure_levels['t'].values[0, :, 0, 0]) == [1000, 925, 850, 700, 600, 500, 400, 300, 250, 200, 150, 100, 50]
	assert list(single_level['longitude'].values) == [270.0, 90.0]
	assert list(pressure_levels['longitude'].values) == [270.0, 90.0]
	assert 'step' not in pressure_levels.coords
	assert list(single_level['time'].values) == [np.datetime64('2026-11-03T06:00', 'ns')]


def test_query_caches_grib_by_url_and_index_entry(monkeypatch):
	calls = []
	monkeypatch.setattr(inputs.cache, 'get', lambda path, key, extension, load, put: calls.append((path, json.loads(key), extension, load)))

	inputs._query('https://mirror/file.grib2', {'_offset': 1, '_length': 2})

	path, key, extension, load = calls[0]
	assert path == inputs._home_path
	assert key == {'https://mirror/file.grib2': {'_offset': 1, '_length': 2}}
	assert extension == 'grib2'
	assert load is xr.load_dataset
