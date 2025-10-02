#  Copyright (c) 2024 INFISYS INC

import json
import os
import requests
import hashlib
import logging

import xarray as xr
import pandas as pd

from datetime import timedelta, datetime, timezone
from graphcast import graphcast, checkpoint
from decorator import retry
from model import Graphcast

from decorator import log

logger = logging.getLogger('graphcast')

_geopotential_meters_to_geopotential = 9.80665

home_path = os.path.join(os.path.expanduser('~'), 'graphcast', 'gfs')
os.makedirs(home_path, exist_ok=True)


class Gfs:
	@staticmethod
	@log
	def model(t0, predictions, model_path, timestep, observed_timesteps):
		logger.info('model load begin')
		with open(os.path.join(model_path, 'model.npz'), 'rb') as f:
			model = checkpoint.load(f, graphcast.CheckPoint)
			logger.info('model load complete')

		logger.info('Gfs._get_single_level download begin')
		single_level = Gfs._get_single_level(t0, timestep, observed_timesteps)
		logger.info('Gfs._get_single_level download complete')
		logger.info('Gfs._get_pressure_levels download begin')
		pressure_levels = Gfs._get_pressure_levels(t0, timestep, observed_timesteps)
		logger.info('Gfs._get_pressure_levels download complete')

		return Graphcast.predict(model, single_level, pressure_levels, t0, timestep, predictions, model_path)

	@staticmethod
	@retry
	def _query(name, request):
		query = json.dumps({name: request})
		filename = os.path.join(home_path, f"{hashlib.sha3_256(query.encode()).hexdigest()}.grib2")
		logger.info(f'Gfs._query begin [{query} -> {filename}]')
		cache_dates = {datetime.fromtimestamp(os.path.getctime(os.path.join(home_path, f)), tz=timezone.utc): f for f in os.listdir(home_path)}
		if os.path.exists(filename):
			try:
				data = xr.load_dataset(filename)
			except:
				Gfs._put_data_cache(name, request, filename)
				data = xr.load_dataset(filename)
		else:
			Gfs._put_data_cache(name, request, filename)
			data = xr.load_dataset(filename)

		if not data['time'].size:
			raise ImportError("Empty response dataset")

		[os.remove(os.path.join(home_path, cache_dates[d])) for d in cache_dates if d < datetime.now(tz=timezone.utc) - timedelta(days=7)]

		logger.info('Gfs._query complete')

		return data

	@staticmethod
	def _put_data_cache(name, request, filename):
		response = requests.get(f'{name}{"".join(request)}', verify=False)
		if response.status_code != 200:
			raise ImportError(response.text)
		with open(filename, 'wb') as f:
			f.write(response.content)

	@staticmethod
	def _get_single_level(t0, timestep, observed_timesteps):
		single_levels = []
		start_time = t0 - timedelta(hours=observed_timesteps * timestep)
		for t in range(observed_timesteps):
			observed = start_time + timedelta(hours=t * timestep)
			name = f'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?dir=/gfs.{observed:%Y%m%d}/{observed:%H}/atmos&file=gfs.t{observed:%H}z.pgrb2.0p25.f000'

			logger.info(f'Gfs._get_single_level {observed} download begin')

			request = ['&var_HGT=on', '&lev_surface=on']
			data = Gfs._query(name, request).drop_vars(['step', 'surface', 'valid_time'])
			data['orog'] = data['orog'] * _geopotential_meters_to_geopotential

			request = ['&var_LAND=on', '&var_PRMSL=on']
			data = xr.merge([data, Gfs._query(name, request).drop_vars(['step', 'surface', 'valid_time', 'meanSea'])])

			request = ['&var_TMP=on', '&lev_2_m_above_ground=on']
			data = xr.merge([data, Gfs._query(name, request).drop_vars(['step', 'valid_time', 'heightAboveGround'])])

			request = ['&var_UGRD=on', '&var_VGRD=on', '&lev_10_m_above_ground=on']
			data = xr.merge([data, Gfs._query(name, request).drop_vars(['step', 'valid_time', 'heightAboveGround'])])

			data = data.expand_dims('time')
			data['time'] = pd.to_datetime(data['time'].values)

			single_levels.append(data.to_dataframe())

			logger.info(f'Gfs._get_single_level {observed} download complete')

		single_level = pd.concat(single_levels)
		single_level = single_level.rename(columns={'orog': 'geopotential_at_surface', 'prmsl': 'mean_sea_level_pressure', 'lsm': 'land_sea_mask', 't2m': '2m_temperature', 'u10': '10m_u_component_of_wind', 'v10': '10m_v_component_of_wind'})
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

			logger.info(f'Gfs._get_pressure_levels {observed} download begin')

			name = f'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?dir=/gfs.{observed:%Y%m%d}/{observed:%H}/atmos&file=gfs.t{observed:%H}z.pgrb2.0p25.f000'
			request = [
				'&var_UGRD=on',
				'&var_VGRD=on',
				'&var_HGT=on',
				'&var_SPFH=on',
				'&var_TMP=on',
				'&var_VVEL=on',
				'&lev_1000_mb=on', '&lev_925_mb=on', '&lev_850_mb=on', '&lev_700_mb=on', '&lev_600_mb=on', '&lev_500_mb=on', '&lev_400_mb=on', '&lev_300_mb=on', '&lev_250_mb=on', '&lev_200_mb=on', '&lev_150_mb=on', '&lev_100_mb=on', '&lev_50_mb=on']
			data = Gfs._query(name, request).drop_vars(['step', 'valid_time'])
			data['gh'] = data['gh'] * _geopotential_meters_to_geopotential

			data = data.expand_dims('time')
			data['time'] = pd.to_datetime(data['time'].values)

			pressure_levels.append(data.to_dataframe())

			logger.info(f'Gfs._get_pressure_levels {observed} download complete')

		pressure_level = pd.concat(pressure_levels)
		pressure_level = pressure_level.rename(columns={'gh': 'geopotential', 't': 'temperature', 'q': 'specific_humidity', 'w': 'vertical_velocity', 'u': 'u_component_of_wind', 'v': 'v_component_of_wind'})
		pressure_level = pressure_level.rename_axis(index={'latitude': 'lat', 'longitude': 'lon', 'isobaricInhPa': 'level'})
		pressure_level = pressure_level.reorder_levels(['time', 'lat', 'lon', 'level']).sort_index()
		pressure_level['batch'] = 0
		pressure_level = pressure_level.set_index('batch', append=True)
		return pressure_level
