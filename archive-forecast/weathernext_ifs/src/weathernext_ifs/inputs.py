#  Copyright (c) 2026 INFISYS INC

import json
import logging

import xarray as xr
import pandas as pd

from datetime import timedelta
from archive_forecast import cache, web
from archive_forecast.graphcast import utils
from archive_forecast.log import log
from archive_forecast.retry import retry

_logger = logging.getLogger('weathernext_ifs')
_home_path = cache.folder('weathernext_ifs')


@log
def read(t0, timestep, observed_timesteps):
	cache.clean(_home_path)
	start_time = t0 - timedelta(hours=observed_timesteps * timestep)
	observed = [_get_observed(start_time + timedelta(hours=t * timestep)) for t in range(observed_timesteps)]

	single_level = xr.concat([o[0] for o in observed], dim='time')
	single_level = single_level.rename({'z': 'geopotential_at_surface', 'msl': 'mean_sea_level_pressure', 'lsm': 'land_sea_mask', 't2m': '2m_temperature', 'u10': '10m_u_component_of_wind', 'v10': '10m_v_component_of_wind', 'latitude': 'lat', 'longitude': 'lon'})
	single_level = single_level.sortby(['lat', 'lon'])

	pressure_levels = xr.concat([o[1] for o in observed], dim='time')
	pressure_levels = pressure_levels.rename({'u': 'u_component_of_wind', 'v': 'v_component_of_wind', 'z': 'geopotential', 'q': 'specific_humidity', 't': 'temperature', 'w': 'vertical_velocity', 'latitude': 'lat', 'longitude': 'lon', 'isobaricInhPa': 'level'})
	pressure_levels = pressure_levels.sortby(['lat', 'lon', 'level'])

	return utils.strip(single_level), utils.strip(pressure_levels)


@retry
def _query(name, request):
	return cache.get(_home_path, json.dumps({name: request}), 'grib2', xr.load_dataset, lambda filename: _put_data_cache(name, request, filename))


@retry
def _get_index(url):
	url = url.replace('.grib2', '.index')
	return cache.get(_home_path, json.dumps({url: None}), 'index', _read_index, lambda filename: _put_index_cache(url, filename))


def _read_index(filename):
	with open(filename, 'r') as f:
		return {tuple(i[k] for k in ['levtype', 'param', 'levelist'] if k in i): i for i in [json.loads(line) for line in f.read().splitlines()]}


def _put_index_cache(url, filename):
	with web.session() as session:
		response = session.get(url, verify=False)
	if response.status_code != 200:
		raise ImportError(response.text)
	with open(filename, 'w') as f:
		f.write(response.text)


def _put_data_cache(url, request, filename):
	with web.session() as session:
		response = session.get(url, headers={'Range': f"bytes={request['_offset']}-{request['_offset'] + request['_length'] - 1}"}, verify=False)
	if response.status_code != 206:
		raise ImportError(response.text)
	with open(filename, 'wb') as f:
		f.write(response.content)


@log
def _get_observed(observed):
	url = f'https://storage.googleapis.com/ecmwf-open-data/{observed:%Y%m%d}/{observed:%H}z/ifs/0p25/oper/{observed:%Y%m%d%H}0000-0h-oper-fc.grib2'
	index = _get_index(url)
	time = [pd.Timestamp(observed).as_unit('ns')]

	_logger.info(f'_get_observed {observed} download begin')

	single_level = xr.merge([_query(url, index[('sfc', parameter)]).reset_coords(drop=True) for parameter in ['z', 'lsm', 'msl', '2t', '10u', '10v']], compat='no_conflicts').expand_dims(time=time)
	single_level = single_level.assign_coords(longitude=single_level['longitude'] % 360)

	pressure_levels = xr.merge([xr.concat([_query(url, index[('pl', parameter, level)]) for level in ['1000', '925', '850', '700', '600', '500', '400', '300', '250', '200', '150', '100', '50']], dim='isobaricInhPa') for parameter in ['u', 'v', 'z', 'q', 't', 'w']], compat='no_conflicts').reset_coords(drop=True).expand_dims(time=time)
	pressure_levels = pressure_levels.assign_coords(longitude=pressure_levels['longitude'] % 360)

	_logger.info(f'_get_observed {observed} download complete')

	return single_level, pressure_levels
