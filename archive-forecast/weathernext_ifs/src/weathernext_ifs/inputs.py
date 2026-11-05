#  Copyright (c) 2026 INFISYS INC

import json
import os
import requests
import hashlib
import logging
import urllib3

import xarray as xr
import pandas as pd

from requests import adapters
from datetime import timedelta, datetime, timezone
from archive_forecast.log import log
from archive_forecast.retry import retry

_logger = logging.getLogger('weathernext_ifs')
_home_path = os.path.join(os.path.expanduser('~'), '.weathernext_ifs')

os.makedirs(_home_path, exist_ok=True)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@log
def read(t0, timestep, observed_timesteps):
	_clean_cache()
	single_level = _get_single_level(t0, timestep, observed_timesteps)
	pressure_levels = _get_pressure_levels(t0, timestep, observed_timesteps)
	return single_level, pressure_levels


@retry
def _query(name, request):
	query = json.dumps({name: request})
	filename = os.path.join(_home_path, f"{hashlib.sha3_256(query.encode()).hexdigest()}.grib2")

	_logger.info(f'_query begin [{query} -> {filename}]')

	if os.path.exists(filename):
		try:
			data = xr.load_dataset(filename)
		except:
			_put_data_cache(name, request, filename)
			data = xr.load_dataset(filename)
	else:
		_put_data_cache(name, request, filename)
		data = xr.load_dataset(filename)

	_logger.info('_query complete')

	return data


@log
def _clean_cache():
	cache_dates = {datetime.fromtimestamp(os.path.getctime(os.path.join(_home_path, f)), tz=timezone.utc): f for f in os.listdir(_home_path)}
	[os.remove(os.path.join(_home_path, cache_dates[d])) for d in cache_dates if d < datetime.now(tz=timezone.utc) - timedelta(days=7)]


@retry
def _get_index(url):
	url = url.replace('.grib2', '.index')
	query = json.dumps({url: None})
	filename = os.path.join(_home_path, f"{hashlib.sha3_256(query.encode()).hexdigest()}.index")

	_logger.info(f'_get_index begin [{query} -> {filename}]')

	if os.path.exists(filename):
		try:
			index = _read_index(filename)
		except:
			_put_index_cache(url, filename)
			index = _read_index(filename)
	else:
		_put_index_cache(url, filename)
		index = _read_index(filename)

	_logger.info('_get_index complete')

	return index


def _read_index(filename):
	with open(filename, 'r') as f:
		return {tuple(i[k] for k in ['levtype', 'param', 'levelist'] if k in i): i for i in [json.loads(line) for line in f.read().splitlines()]}


def _put_index_cache(url, filename):
	with requests.Session() as session:
		session.mount('https://', adapters.HTTPAdapter(max_retries=urllib3.util.retry.Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])))
		response = session.get(url, verify=False)
	if response.status_code != 200:
		raise ImportError(response.text)
	with open(filename, 'w') as f:
		f.write(response.text)


def _put_data_cache(url, request, filename):
	with requests.Session() as session:
		session.mount('https://', adapters.HTTPAdapter(max_retries=urllib3.util.retry.Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])))
		response = session.get(url, headers={'Range': f"bytes={request['_offset']}-{request['_offset'] + request['_length'] - 1}"}, verify=False)
	if response.status_code != 206:
		raise ImportError(response.text)
	with open(filename, 'wb') as f:
		f.write(response.content)


@log
def _get_single_level(t0, timestep, observed_timesteps):
	single_levels = []
	start_time = t0 - timedelta(hours=observed_timesteps * timestep)
	for t in range(observed_timesteps):
		observed = start_time + timedelta(hours=t * timestep)
		url = f'https://storage.googleapis.com/ecmwf-open-data/{observed:%Y%m%d}/{observed:%H}z/ifs/0p25/oper/{observed:%Y%m%d%H}0000-0h-oper-fc.grib2'
		index = _get_index(url)

		_logger.info(f'_get_single_level {observed} download begin')

		data = xr.merge([_query(url, index[('sfc', parameter)]).reset_coords(drop=True) for parameter in ['z', 'lsm', 'msl', '2t', '10u', '10v']], compat='no_conflicts').expand_dims(time=[pd.Timestamp(observed).as_unit('ns')])
		data = data.assign_coords(longitude=data['longitude'] % 360)

		single_levels.append(data)

		_logger.info(f'_get_single_level {observed} download complete')

	single_level = xr.concat(single_levels, dim='time')
	single_level = single_level.rename({'z': 'geopotential_at_surface', 'msl': 'mean_sea_level_pressure', 'lsm': 'land_sea_mask', 't2m': '2m_temperature', 'u10': '10m_u_component_of_wind', 'v10': '10m_v_component_of_wind', 'latitude': 'lat', 'longitude': 'lon'})
	single_level = single_level.sortby(['lat', 'lon'])
	return xr.Dataset({v: (single_level[v].dims, single_level[v].values) for v in single_level.data_vars}, coords={c: single_level[c].values for c in single_level.dims})


@log
def _get_pressure_levels(t0, timestep, observed_timesteps):
	pressure_levels = []
	start_time = t0 - timedelta(hours=observed_timesteps * timestep)
	for t in range(observed_timesteps):
		observed = start_time + timedelta(hours=t * timestep)
		url = f'https://storage.googleapis.com/ecmwf-open-data/{observed:%Y%m%d}/{observed:%H}z/ifs/0p25/oper/{observed:%Y%m%d%H}0000-0h-oper-fc.grib2'
		index = _get_index(url)

		_logger.info(f'_get_pressure_levels {observed} download begin')

		data = xr.merge([xr.concat([_query(url, index[('pl', parameter, level)]) for level in ['1000', '925', '850', '700', '600', '500', '400', '300', '250', '200', '150', '100', '50']], dim='isobaricInhPa') for parameter in ['u', 'v', 'z', 'q', 't', 'w']], compat='no_conflicts').reset_coords(drop=True).expand_dims(time=[pd.Timestamp(observed).as_unit('ns')])
		data = data.assign_coords(longitude=data['longitude'] % 360)

		pressure_levels.append(data)

		_logger.info(f'_get_pressure_levels {observed} download complete')

	pressure_level = xr.concat(pressure_levels, dim='time')
	pressure_level = pressure_level.rename({'u': 'u_component_of_wind', 'v': 'v_component_of_wind', 'z': 'geopotential', 'q': 'specific_humidity', 't': 'temperature', 'w': 'vertical_velocity', 'latitude': 'lat', 'longitude': 'lon', 'isobaricInhPa': 'level'})
	pressure_level = pressure_level.sortby(['lat', 'lon', 'level'])
	return xr.Dataset({v: (pressure_level[v].dims, pressure_level[v].values) for v in pressure_level.data_vars}, coords={c: pressure_level[c].values for c in pressure_level.dims})
