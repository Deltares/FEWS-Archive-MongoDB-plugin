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
from weathernext_ifs.decorator import retry, log

logger = logging.getLogger('weathernext_ifs')
home_path = os.path.join(os.path.expanduser('~'), 'weathernext_ifs')
os.makedirs(home_path, exist_ok=True)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@log
def read(t0, timestep, observed_timesteps):
	single_level = _get_single_level(t0, timestep, observed_timesteps)
	pressure_levels = _get_pressure_levels(t0, timestep, observed_timesteps)
	return single_level, pressure_levels


@retry
def _query(name, request):
	query = json.dumps({name: request})
	filename = os.path.join(home_path, f"{hashlib.sha3_256(query.encode()).hexdigest()}.grib2")
	logger.info(f'_query begin [{query} -> {filename}]')
	cache_dates = {datetime.fromtimestamp(os.path.getctime(os.path.join(home_path, f)), tz=timezone.utc): f for f in os.listdir(home_path)}
	if os.path.exists(filename):
		try:
			data = xr.load_dataset(filename)
		except:
			_put_data_cache(name, request, filename)
			data = xr.load_dataset(filename)
	else:
		_put_data_cache(name, request, filename)
		data = xr.load_dataset(filename)

	[os.remove(os.path.join(home_path, cache_dates[d])) for d in cache_dates if d < datetime.now(tz=timezone.utc) - timedelta(days=7)]

	logger.info('_query complete')

	return data


def _get_index(name):
	with requests.Session() as session:
		session.mount('https://', adapters.HTTPAdapter(max_retries=urllib3.util.retry.Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])))
		response = session.get(name.replace('.grib2', '.index'), verify=False)
	if response.status_code != 200:
		raise ImportError(response.text)
	return {tuple(i[k] for k in ['levtype', 'param', 'levelist'] if k in i): i for i in [json.loads(line) for line in response.text.splitlines()]}


def _put_data_cache(name, request, filename):
	with requests.Session() as session:
		session.mount('https://', adapters.HTTPAdapter(max_retries=urllib3.util.retry.Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])))
		response = session.get(name, headers={'Range': f"bytes={request['_offset']}-{request['_offset'] + request['_length'] - 1}"}, verify=False)
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
		logger.info(f'_get_single_level {observed} download begin')
		name = f'https://ecmwf-forecasts.s3.eu-central-1.amazonaws.com/{observed:%Y%m%d}/{observed:%H}z/ifs/0p25/oper/{observed:%Y%m%d%H}0000-0h-oper-fc.grib2'
		index = _get_index(name)

		data = xr.merge([_query(name, index[('sfc', parameter)]).reset_coords(drop=True) for parameter in ['z', 'lsm', 'msl', '2t', '10u', '10v']], compat='no_conflicts').expand_dims(time=[pd.Timestamp(observed).as_unit('ns')])
		data = data.assign_coords(longitude=data['longitude'] % 360)

		single_levels.append(data.to_dataframe())

		logger.info(f'_get_single_level {observed} download complete')

	single_level = pd.concat(single_levels)
	single_level = single_level.rename(columns={'z': 'geopotential_at_surface', 'msl': 'mean_sea_level_pressure', 'lsm': 'land_sea_mask', 't2m': '2m_temperature', 'u10': '10m_u_component_of_wind', 'v10': '10m_v_component_of_wind'})
	single_level = single_level.rename_axis(index={'latitude': 'lat', 'longitude': 'lon'})
	single_level = single_level.reorder_levels(['time', 'lat', 'lon']).sort_index()
	single_level['batch'] = 0
	single_level = single_level.set_index('batch', append=True)
	return single_level


@log
def _get_pressure_levels(t0, timestep, observed_timesteps):
	pressure_levels = []
	start_time = t0 - timedelta(hours=observed_timesteps * timestep)
	for t in range(observed_timesteps):
		observed = start_time + timedelta(hours=t * timestep)
		logger.info(f'_get_pressure_levels {observed} download begin')
		name = f'https://ecmwf-forecasts.s3.eu-central-1.amazonaws.com/{observed:%Y%m%d}/{observed:%H}z/ifs/0p25/oper/{observed:%Y%m%d%H}0000-0h-oper-fc.grib2'
		index = _get_index(name)

		data = xr.merge([xr.concat([_query(name, index[('pl', parameter, level)]) for level in ['1000', '925', '850', '700', '600', '500', '400', '300', '250', '200', '150', '100', '50']], dim='isobaricInhPa') for parameter in ['u', 'v', 'z', 'q', 't', 'w']], compat='no_conflicts').reset_coords(drop=True).expand_dims(time=[pd.Timestamp(observed).as_unit('ns')])
		data = data.assign_coords(longitude=data['longitude'] % 360)

		pressure_levels.append(data.to_dataframe())

		logger.info(f'_get_pressure_levels {observed} download complete')

	pressure_level = pd.concat(pressure_levels)
	pressure_level = pressure_level.rename(columns={'u': 'u_component_of_wind', 'v': 'v_component_of_wind', 'z': 'geopotential', 'q': 'specific_humidity', 't': 'temperature', 'w': 'vertical_velocity'})
	pressure_level = pressure_level.rename_axis(index={'latitude': 'lat', 'longitude': 'lon', 'isobaricInhPa': 'level'})
	pressure_level = pressure_level.reorder_levels(['time', 'lat', 'lon', 'level']).sort_index()
	pressure_level['batch'] = 0
	pressure_level = pressure_level.set_index('batch', append=True)
	return pressure_level
