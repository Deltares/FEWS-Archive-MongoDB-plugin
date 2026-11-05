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

_geopotential_meters_to_geopotential = 9.80665
_logger = logging.getLogger('graphcast_gfs')
_home_path = cache.folder('graphcast_gfs')


@log
def read(t0, timestep, observed_timesteps):
	cache.clean(_home_path)
	start_time = t0 - timedelta(hours=observed_timesteps * timestep)
	observed = [_get_observed(start_time + timedelta(hours=t * timestep)) for t in range(observed_timesteps)]

	single_level = xr.concat([o[0] for o in observed], dim='time')
	single_level = single_level.rename({'orog': 'geopotential_at_surface', 'prmsl': 'mean_sea_level_pressure', 'lsm': 'land_sea_mask', 't2m': '2m_temperature', 'u10': '10m_u_component_of_wind', 'v10': '10m_v_component_of_wind', 'latitude': 'lat', 'longitude': 'lon'})
	single_level = single_level.sortby(['lat', 'lon'])

	pressure_levels = xr.concat([o[1] for o in observed], dim='time')
	pressure_levels = pressure_levels.rename({'gh': 'geopotential', 't': 'temperature', 'q': 'specific_humidity', 'w': 'vertical_velocity', 'u': 'u_component_of_wind', 'v': 'v_component_of_wind', 'latitude': 'lat', 'longitude': 'lon', 'isobaricInhPa': 'level'})
	pressure_levels = pressure_levels.sortby(['lat', 'lon', 'level'])

	return utils.strip(single_level), utils.strip(pressure_levels)


@retry
def _query(name, request):
	return cache.get(_home_path, json.dumps({name: request}), 'grib2', xr.load_dataset, lambda filename: _put_cache(name, request, filename))


def _put_cache(url, request, filename):
	with web.session() as session:
		response = session.get(f'{url}{"".join(request)}', verify=False)
	if response.status_code != 200:
		raise ImportError(response.text)
	with open(filename, 'wb') as f:
		f.write(response.content)


@log
def _get_observed(observed):
	url = f'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?dir=/gfs.{observed:%Y%m%d}/{observed:%H}/atmos&file=gfs.t{observed:%H}z.pgrb2.0p25.f000'
	time = [pd.Timestamp(observed).as_unit('ns')]

	_logger.info(f'_get_observed {observed} download begin')

	single_level = xr.merge([_query(url, request).reset_coords(drop=True) for request in [
		['&var_HGT=on', '&lev_surface=on'],
		['&var_LAND=on', '&var_PRMSL=on'],
		['&var_TMP=on', '&lev_2_m_above_ground=on'],
		['&var_UGRD=on', '&var_VGRD=on', '&lev_10_m_above_ground=on']]], compat='no_conflicts').expand_dims(time=time)
	single_level['orog'] = single_level['orog'] * _geopotential_meters_to_geopotential

	request = [
		'&var_UGRD=on',
		'&var_VGRD=on',
		'&var_HGT=on',
		'&var_SPFH=on',
		'&var_TMP=on',
		'&var_VVEL=on',
		'&lev_1000_mb=on', '&lev_925_mb=on', '&lev_850_mb=on', '&lev_700_mb=on', '&lev_600_mb=on', '&lev_500_mb=on', '&lev_400_mb=on', '&lev_300_mb=on', '&lev_250_mb=on', '&lev_200_mb=on', '&lev_150_mb=on', '&lev_100_mb=on', '&lev_50_mb=on']
	pressure_levels = _query(url, request).reset_coords(drop=True).expand_dims(time=time)
	pressure_levels['gh'] = pressure_levels['gh'] * _geopotential_meters_to_geopotential

	_logger.info(f'_get_observed {observed} download complete')

	return single_level, pressure_levels
