#  Copyright (c) 2026 INFISYS INC

import json
import os
from ecmwf.datastores import Client
import hashlib
import logging
import zipfile
import tempfile

import xarray as xr
import pandas as pd

from datetime import timedelta, datetime, timezone
from weathernext_ifs.decorator import retry, log

logger = logging.getLogger('weathernext_ifs')

home_path = os.path.join(os.path.expanduser('~'), 'weathernext_ifs')
os.makedirs(home_path, exist_ok=True)


@log
def read(t0, timestep, observed_timesteps):
	logger.info('_get_single_level download begin')
	single_level = _get_single_level(t0, timestep, observed_timesteps)
	logger.info('_get_single_level download complete')

	logger.info('_get_pressure_levels download begin')
	pressure_levels = _get_pressure_levels(t0, timestep, observed_timesteps)
	logger.info('_get_pressure_levels download complete')

	return single_level, pressure_levels


@retry
def _query(name, request):
	query = json.dumps({name: request})
	filename = os.path.join(home_path, f"{hashlib.sha3_256(query.encode()).hexdigest()}.nc")
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

	if not data['valid_time'].size:
		raise ImportError("Empty response dataset")

	[os.remove(os.path.join(home_path, cache_dates[d])) for d in cache_dates if d < datetime.now(tz=timezone.utc) - timedelta(days=7)]

	logger.info('_query complete')

	return data


def _put_data_cache(name, request, filename):
	Client().retrieve(name, request, filename)
	if zipfile.is_zipfile(filename):
		with tempfile.TemporaryDirectory() as d:
			with zipfile.ZipFile(filename) as z:
				z.extractall(d)
				data = xr.merge([xr.load_dataset(os.path.join(d, n)) for n in z.namelist()], compat='no_conflicts')
		data.to_netcdf(filename, format='NETCDF4', encoding={v: {'zlib': True, 'complevel': 4} for v in data.data_vars})


def _get_single_level(t0, timestep, observed_timesteps):
	single_levels = []
	start_time = t0 - timedelta(hours=observed_timesteps * timestep)
	for t in range(observed_timesteps):
		observed = start_time + timedelta(hours=t * timestep)

		logger.info(f'_get_single_level {observed} download begin')

		name = 'reanalysis-era5-single-levels'
		request = {
			'product_type': 'reanalysis',
			'variable': ['10m_u_component_of_wind', '10m_v_component_of_wind', '2m_temperature', 'geopotential', 'land_sea_mask', 'mean_sea_level_pressure', 'toa_incident_solar_radiation'],
			'grid': '0.25/0.25',
			'year': [observed.year],
			'month': [observed.month],
			'day': [observed.day],
			'time': [observed.strftime('%H:%M')],
			'data_format': 'netcdf',
			'download_format': 'unarchived'}
		data = _query(name, request)
		data = data.drop_vars(set(data.variables) - {'valid_time', 'latitude', 'longitude', 'lsm', 'msl', 't2m', 'time', 'tisr', 'u10', 'v10', 'z'}).rename({'valid_time': 'time'})
		data = data.rename({'u10': '10m_u_component_of_wind', 'v10': '10m_v_component_of_wind', 't2m': '2m_temperature', 'z': 'geopotential_at_surface', 'lsm': 'land_sea_mask', 'msl': 'mean_sea_level_pressure', 'tisr': 'toa_incident_solar_radiation'})
		data['time'] = pd.to_datetime(data['time'].values)

		single_levels.append(data.to_dataframe())

		logger.info(f'_get_single_level {observed} download complete')

	single_level = pd.concat(single_levels)
	single_level = single_level.rename_axis(index={'latitude': 'lat', 'longitude': 'lon'})
	single_level = single_level.reorder_levels(['time', 'lat', 'lon']).sort_index()
	single_level['batch'] = 0
	single_level = single_level.set_index('batch', append=True)
	return single_level


def _get_pressure_levels(t0, timestep, observed_timesteps):
	pressure_levels = []
	start_time = t0 - timedelta(hours=observed_timesteps * timestep)
	for t in range(observed_timesteps):
		observed = start_time + timedelta(hours=t * timestep)

		logger.info(f'_get_pressure_levels {observed} download begin')

		name = 'reanalysis-era5-pressure-levels'
		request = {
			'product_type': 'reanalysis',
			'variable': ['u_component_of_wind', 'v_component_of_wind', 'geopotential', 'specific_humidity', 'temperature', 'vertical_velocity'],
			'grid': '0.25/0.25',
			'year': [observed.year],
			'month': [observed.month],
			'day': [observed.day],
			'time': [observed.strftime('%H:%M')],
			'pressure_level': [1000, 925, 850, 700, 600, 500, 400, 300, 250, 200, 150, 100, 50],
			'data_format': 'netcdf',
			'download_format': 'unarchived'}
		data = _query(name, request)
		data = data.drop_vars(set(data.variables) - {'valid_time', 'latitude', 'longitude', 'pressure_level', 'q', 't', 'u', 'v', 'w', 'z'}).rename({'valid_time': 'time'})
		data = data.rename({'u': 'u_component_of_wind', 'v': 'v_component_of_wind', 'z': 'geopotential', 'q': 'specific_humidity', 't': 'temperature', 'w': 'vertical_velocity'})
		data['time'] = pd.to_datetime(data['time'].values)

		pressure_levels.append(data.to_dataframe())

		logger.info(f'_get_pressure_levels {observed} download complete')

	pressure_level = pd.concat(pressure_levels)
	pressure_level = pressure_level.rename_axis(index={'latitude': 'lat', 'longitude': 'lon', 'pressure_level': 'level'})
	pressure_level = pressure_level.reorder_levels(['time', 'lat', 'lon', 'level']).sort_index()
	pressure_level['batch'] = 0
	pressure_level = pressure_level.set_index('batch', append=True)
	return pressure_level
