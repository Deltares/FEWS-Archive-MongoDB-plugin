#  Copyright (c) 2026 INFISYS INC

import os
import json
import hashlib
import logging
import urllib3
import requests

import xarray as xr
import pandas as pd

from requests import adapters
from datetime import timedelta, datetime, timezone
from graphcast_gfs.decorator import retry, log

geopotential_meters_to_geopotential = 9.80665
logger = logging.getLogger('graphcast_gfs')
home_path = os.path.join(os.path.expanduser('~'), 'graphcast_gfs')
os.makedirs(home_path, exist_ok=True)
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
	filename = os.path.join(home_path, f"{hashlib.sha3_256(query.encode()).hexdigest()}.grib2")

	logger.info(f'_query begin [{query} -> {filename}]')

	if os.path.exists(filename):
		try:
			data = xr.load_dataset(filename)
		except:
			_put_cache(name, request, filename)
			data = xr.load_dataset(filename)
	else:
		_put_cache(name, request, filename)
		data = xr.load_dataset(filename)

	logger.info('_query complete')

	return data


@log
def _clean_cache():
	cache_dates = {datetime.fromtimestamp(os.path.getctime(os.path.join(home_path, f)), tz=timezone.utc): f for f in os.listdir(home_path)}
	[os.remove(os.path.join(home_path, cache_dates[d])) for d in cache_dates if d < datetime.now(tz=timezone.utc) - timedelta(days=7)]


def _put_cache(url, request, filename):
	with requests.Session() as session:
		session.mount('https://', adapters.HTTPAdapter(max_retries=urllib3.util.retry.Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])))
		response = session.get(f'{url}{"".join(request)}', verify=False)
	if response.status_code != 200:
		raise ImportError(response.text)
	with open(filename, 'wb') as f:
		f.write(response.content)


@log
def _get_single_level(t0, timestep, observed_timesteps):
	single_levels = []
	start_time = t0 - timedelta(hours=observed_timesteps * timestep)
	for t in range(observed_timesteps):
		observed = start_time + timedelta(hours=t * timestep)
		url = f'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?dir=/gfs.{observed:%Y%m%d}/{observed:%H}/atmos&file=gfs.t{observed:%H}z.pgrb2.0p25.f000'

		logger.info(f'_get_single_level {observed} download begin')

		data = xr.merge([_query(url, request).reset_coords(drop=True) for request in [
			['&var_HGT=on', '&lev_surface=on'],
			['&var_LAND=on', '&var_PRMSL=on'],
			['&var_TMP=on', '&lev_2_m_above_ground=on'],
			['&var_UGRD=on', '&var_VGRD=on', '&lev_10_m_above_ground=on']]], compat='no_conflicts').expand_dims(time=[pd.Timestamp(observed).as_unit('ns')])

		data['orog'] = data['orog'] * geopotential_meters_to_geopotential
		single_levels.append(data.to_dataframe())

		logger.info(f'_get_single_level {observed} download complete')

	single_level = pd.concat(single_levels)
	single_level = single_level.rename(columns={'orog': 'geopotential_at_surface', 'prmsl': 'mean_sea_level_pressure', 'lsm': 'land_sea_mask', 't2m': '2m_temperature', 'u10': '10m_u_component_of_wind', 'v10': '10m_v_component_of_wind'})
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
		url = f'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?dir=/gfs.{observed:%Y%m%d}/{observed:%H}/atmos&file=gfs.t{observed:%H}z.pgrb2.0p25.f000'

		logger.info(f'_get_pressure_levels {observed} download begin')

		request = [
			'&var_UGRD=on',
			'&var_VGRD=on',
			'&var_HGT=on',
			'&var_SPFH=on',
			'&var_TMP=on',
			'&var_VVEL=on',
			'&lev_1000_mb=on', '&lev_925_mb=on', '&lev_850_mb=on', '&lev_700_mb=on', '&lev_600_mb=on', '&lev_500_mb=on', '&lev_400_mb=on', '&lev_300_mb=on', '&lev_250_mb=on', '&lev_200_mb=on', '&lev_150_mb=on', '&lev_100_mb=on', '&lev_50_mb=on']
		data = _query(url, request).reset_coords(drop=True).expand_dims(time=[pd.Timestamp(observed).as_unit('ns')])
		data['gh'] = data['gh'] * geopotential_meters_to_geopotential

		pressure_levels.append(data.to_dataframe())

		logger.info(f'_get_pressure_levels {observed} download complete')

	pressure_level = pd.concat(pressure_levels)
	pressure_level = pressure_level.rename(columns={'gh': 'geopotential', 't': 'temperature', 'q': 'specific_humidity', 'w': 'vertical_velocity', 'u': 'u_component_of_wind', 'v': 'v_component_of_wind'})
	pressure_level = pressure_level.rename_axis(index={'latitude': 'lat', 'longitude': 'lon', 'isobaricInhPa': 'level'})
	pressure_level = pressure_level.reorder_levels(['time', 'lat', 'lon', 'level']).sort_index()
	pressure_level['batch'] = 0
	pressure_level = pressure_level.set_index('batch', append=True)
	return pressure_level
