#  Copyright (c) 2026 INFISYS INC

import json
import logging

import xarray as xr
import pandas as pd

from datetime import timedelta
from archive_forecast import cache, web
from archive_forecast.log import log
from archive_forecast.retry import retry

_geopotential_meters_to_geopotential = 9.80665
_logger = logging.getLogger('graphcast_gfs')
_home_path = cache.folder('graphcast_gfs')


@log
def read(t0, timestep, observed_timesteps):
	cache.clean(_home_path)
	single_level = _get_single_level(t0, timestep, observed_timesteps)
	pressure_levels = _get_pressure_levels(t0, timestep, observed_timesteps)
	return single_level, pressure_levels


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
def _get_single_level(t0, timestep, observed_timesteps):
	single_levels = []
	start_time = t0 - timedelta(hours=observed_timesteps * timestep)
	for t in range(observed_timesteps):
		observed = start_time + timedelta(hours=t * timestep)
		url = f'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?dir=/gfs.{observed:%Y%m%d}/{observed:%H}/atmos&file=gfs.t{observed:%H}z.pgrb2.0p25.f000'

		_logger.info(f'_get_single_level {observed} download begin')

		data = xr.merge([_query(url, request).reset_coords(drop=True) for request in [
			['&var_HGT=on', '&lev_surface=on'],
			['&var_LAND=on', '&var_PRMSL=on'],
			['&var_TMP=on', '&lev_2_m_above_ground=on'],
			['&var_UGRD=on', '&var_VGRD=on', '&lev_10_m_above_ground=on']]], compat='no_conflicts').expand_dims(time=[pd.Timestamp(observed).as_unit('ns')])

		data['orog'] = data['orog'] * _geopotential_meters_to_geopotential
		single_levels.append(data)

		_logger.info(f'_get_single_level {observed} download complete')

	single_level = xr.concat(single_levels, dim='time')
	single_level = single_level.rename({'orog': 'geopotential_at_surface', 'prmsl': 'mean_sea_level_pressure', 'lsm': 'land_sea_mask', 't2m': '2m_temperature', 'u10': '10m_u_component_of_wind', 'v10': '10m_v_component_of_wind', 'latitude': 'lat', 'longitude': 'lon'})
	single_level = single_level.sortby(['lat', 'lon'])
	return xr.Dataset({v: (single_level[v].dims, single_level[v].values) for v in single_level.data_vars}, coords={c: single_level[c].values for c in single_level.dims})


@log
def _get_pressure_levels(t0, timestep, observed_timesteps):
	pressure_levels = []
	start_time = t0 - timedelta(hours=observed_timesteps * timestep)
	for t in range(observed_timesteps):
		observed = start_time + timedelta(hours=t * timestep)
		url = f'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?dir=/gfs.{observed:%Y%m%d}/{observed:%H}/atmos&file=gfs.t{observed:%H}z.pgrb2.0p25.f000'

		_logger.info(f'_get_pressure_levels {observed} download begin')

		request = [
			'&var_UGRD=on',
			'&var_VGRD=on',
			'&var_HGT=on',
			'&var_SPFH=on',
			'&var_TMP=on',
			'&var_VVEL=on',
			'&lev_1000_mb=on', '&lev_925_mb=on', '&lev_850_mb=on', '&lev_700_mb=on', '&lev_600_mb=on', '&lev_500_mb=on', '&lev_400_mb=on', '&lev_300_mb=on', '&lev_250_mb=on', '&lev_200_mb=on', '&lev_150_mb=on', '&lev_100_mb=on', '&lev_50_mb=on']
		data = _query(url, request).reset_coords(drop=True).expand_dims(time=[pd.Timestamp(observed).as_unit('ns')])
		data['gh'] = data['gh'] * _geopotential_meters_to_geopotential

		pressure_levels.append(data)

		_logger.info(f'_get_pressure_levels {observed} download complete')

	pressure_level = xr.concat(pressure_levels, dim='time')
	pressure_level = pressure_level.rename({'gh': 'geopotential', 't': 'temperature', 'q': 'specific_humidity', 'w': 'vertical_velocity', 'u': 'u_component_of_wind', 'v': 'v_component_of_wind', 'latitude': 'lat', 'longitude': 'lon', 'isobaricInhPa': 'level'})
	pressure_level = pressure_level.sortby(['lat', 'lon', 'level'])
	return xr.Dataset({v: (pressure_level[v].dims, pressure_level[v].values) for v in pressure_level.data_vars}, coords={c: pressure_level[c].values for c in pressure_level.dims})
