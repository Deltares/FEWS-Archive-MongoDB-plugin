#  Copyright (c) 2024 INFISYS INC

import hashlib
import json
import os
import cdsapi
import logging

import xarray as xr
import pandas as pd

from datetime import timedelta, datetime, timezone
from graphcast import graphcast, checkpoint
from decorator import retry
from model import Graphcast

from decorator import log

logger = logging.getLogger('graphcast')

home_path = os.path.join(os.path.expanduser('~'), 'graphcast', 'ifs')
os.makedirs(home_path, exist_ok=True)


class Ifs:
	@staticmethod
	@log
	def model(t0, predictions, model_path, timestep, observed_timesteps):
		logger.info('model load begin')
		with open(os.path.join(model_path, 'model.npz'), 'rb') as f:
			model = checkpoint.load(f, graphcast.CheckPoint)
			logger.info('model load complete')

		logger.info('Ifs._get_single_level download begin')
		single_level = Ifs._get_single_level(t0, timestep, observed_timesteps)
		logger.info('Ifs._get_single_level download complete')
		logger.info('Ifs._get_pressure_levels download begin')
		pressure_levels = Ifs._get_pressure_levels(t0, timestep, observed_timesteps)
		logger.info('Ifs._get_pressure_levels download complete')

		return Graphcast.predict(model, single_level, pressure_levels, t0, timestep, predictions, model_path)

	@staticmethod
	@retry
	def _query(name, request):
		query = json.dumps({name: request})
		filename = os.path.join(home_path, f"{hashlib.sha3_256(query.encode()).hexdigest()}.nc")
		logger.info(f'Ifs._query begin [{query} -> {filename}]')
		cache_dates = {datetime.fromtimestamp(os.path.getctime(os.path.join(home_path, f)), tz=timezone.utc): f for f in os.listdir(home_path)}
		if os.path.exists(filename):
			try:
				data = xr.load_dataset(filename)
			except:
				Ifs._put_data_cache(name, request, filename)
				data = xr.load_dataset(filename)
		else:
			Ifs._put_data_cache(name, request, filename)
			data = xr.load_dataset(filename)

		if not data['valid_time'].size:
			raise ImportError("Empty response dataset")

		[os.remove(os.path.join(home_path, cache_dates[d])) for d in cache_dates if d < datetime.now(tz=timezone.utc) - timedelta(days=7)]

		logger.info('Ifs._query complete')

		return data

	@staticmethod
	def _put_data_cache(name, request, filename):
		cdsapi.Client().retrieve(name, request, filename)

	@staticmethod
	def _get_single_level(t0, timestep, observed_timesteps):
		single_levels = []
		start_time = t0 - timedelta(hours=observed_timesteps * timestep)
		for t in range(observed_timesteps):
			observed = start_time + timedelta(hours=t * timestep)

			logger.info(f'Ifs._get_single_level {observed} download begin')

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
			data = Ifs._query(name, request)
			data = data.drop_vars(set(data.variables) - {'valid_time', 'latitude', 'longitude', 'lsm', 'msl', 't2m', 'time', 'tisr', 'u10', 'v10', 'z'}).rename({'valid_time': 'time'})
			data = data.rename({'u10': '10m_u_component_of_wind', 'v10': '10m_v_component_of_wind', 't2m': '2m_temperature', 'z': 'geopotential_at_surface', 'lsm': 'land_sea_mask', 'msl': 'mean_sea_level_pressure', 'tisr': 'toa_incident_solar_radiation'})
			data['time'] = pd.to_datetime(data['time'].values)

			single_levels.append(data.to_dataframe())

			logger.info(f'Ifs._get_single_level {observed} download complete')

		single_level = pd.concat(single_levels)
		single_level = single_level.rename_axis(index={'latitude': 'lat', 'longitude': 'lon'})
		single_level = single_level.reorder_levels(['time', 'lat', 'lon']).sort_index()
		single_level['batch'] = 0
		single_level = single_level.set_index('batch', append=True)
		return single_level

	@staticmethod
	def _get_pressure_levels(t0, timestep, observed_timesteps):
		pressure_levels = []
		start_time = t0 - timedelta(hours=observed_timesteps * timestep)
		for t in range(observed_timesteps):
			observed = start_time + timedelta(hours=t * timestep)

			logger.info(f'Ifs._get_pressure_levels {observed} download begin')

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
			data = Ifs._query(name, request)
			data = data.drop_vars(set(data.variables) - {'valid_time', 'latitude', 'longitude', 'pressure_level', 'q', 't', 'u', 'v', 'w', 'z'}).rename({'valid_time': 'time'})
			data = data.rename({'u': 'u_component_of_wind', 'v': 'v_component_of_wind', 'z': 'geopotential', 'q': 'specific_humidity', 't': 'temperature', 'w': 'vertical_velocity'})
			data['time'] = pd.to_datetime(data['time'].values)

			pressure_levels.append(data.to_dataframe())

			logger.info(f'Ifs._get_pressure_levels {observed} download complete')

		pressure_level = pd.concat(pressure_levels)
		pressure_level = pressure_level.rename_axis(index={'latitude': 'lat', 'longitude': 'lon', 'pressure_level': 'level'})
		pressure_level = pressure_level.reorder_levels(['time', 'lat', 'lon', 'level']).sort_index()
		pressure_level['batch'] = 0
		pressure_level = pressure_level.set_index('batch', append=True)
		return pressure_level
