#  Copyright (c) 2026 INFISYS INC

import os
import json
import hashlib
import logging

import zarr
import obstore
import xarray as xr
import pandas as pd

from obstore.store import GCSStore
from datetime import timedelta, datetime, timezone
from weathernext_3.decorator import retry, log

_logger = logging.getLogger('weathernext_3')
_home_path = os.path.join(os.path.expanduser('~'), 'weathernext_3')
os.makedirs(_home_path, exist_ok=True)

_bucket = 'weathernext3_spatial'
_runs_path = 'weathernext_3_0_0/zarr/2026_to_present'

_single_level_fields = {
	'2m_temperature': 'temperature_2m',
	'10m_u_component_of_wind': 'u_component_of_wind_10m',
	'10m_v_component_of_wind': 'v_component_of_wind_10m',
	'mean_sea_level_pressure': 'mean_sea_level_pressure',
	'total_precipitation_1hr': 'total_precipitation_1hr',
	'imerg_tp_1hr': 'imerg_tp_1hr',
	'experimental_tp_1hr': 'experimental_tp_1hr'}

_pressure_level_fields = {
	'u_component_of_wind': 'u_component_of_wind',
	'v_component_of_wind': 'v_component_of_wind',
	'geopotential': 'geopotential',
	'specific_humidity': 'specific_humidity',
	'temperature': 'temperature',
	'vertical_velocity': 'vertical_velocity'}


@log
def read(t0, timestep, predictions, levels, fields, corners, billing_project):
	end = t0 + timedelta(hours=(sum(predictions) - 1) * timestep)
	single_level_fields = [f for f in fields if f in _single_level_fields]
	pressure_level_fields = [f for f in fields if f in _pressure_level_fields]

	run = f'{_runs_path}/{t0 - timedelta(hours=timestep):%Y%m%d_%H}hr_01_preds'

	_logger.info('_get_single_level download begin')
	single_level = _get_single_level(run, t0, end, single_level_fields, corners, billing_project) if single_level_fields else None
	_logger.info('_get_single_level download complete')

	_logger.info('_get_pressure_levels download begin')
	pressure_levels = _get_pressure_levels(run, t0, end, pressure_level_fields, levels, corners, billing_project) if pressure_level_fields else None
	_logger.info('_get_pressure_levels download complete')

	return single_level, pressure_levels


@retry
def _query(name, request, billing_project):
	query = json.dumps({name: request})
	filename = os.path.join(_home_path, f"{hashlib.sha3_256(query.encode()).hexdigest()}.nc")
	_logger.info(f'_query begin [{query} -> {filename}]')
	cache_dates = {datetime.fromtimestamp(os.path.getctime(os.path.join(_home_path, f)), tz=timezone.utc): f for f in os.listdir(_home_path)}
	if os.path.exists(filename):
		try:
			data = xr.load_dataset(filename)
		except:
			_put_data_cache(name, request, billing_project, filename)
			data = xr.load_dataset(filename)
	else:
		_put_data_cache(name, request, billing_project, filename)
		data = xr.load_dataset(filename)

	if not data['lead_time'].size:
		raise ImportError("Empty response dataset")

	[os.remove(os.path.join(_home_path, cache_dates[d])) for d in cache_dates if d < datetime.now(tz=timezone.utc) - timedelta(days=7)]

	_logger.info('_query complete')

	return data


def _put_data_cache(name, request, billing_project, filename):
	store = GCSStore(_bucket, prefix=name, client_options={'default_headers': {'x-goog-user-project': billing_project}, 'allow_invalid_certificates': True}, retry_config={'max_retries': 5, 'backoff': {'init_backoff': timedelta(seconds=1), 'base': 2}})
	obstore.head(store, 'success')
	store = GCSStore(_bucket, prefix=f'{name}/predictions.zarr', client_options={'default_headers': {'x-goog-user-project': billing_project}, 'allow_invalid_certificates': True}, retry_config={'max_retries': 5, 'backoff': {'init_backoff': timedelta(seconds=1), 'base': 2}})
	data = xr.open_zarr(zarr.storage.ObjectStore(store, read_only=True), chunks={})
	init = pd.Timestamp(data['init_time'].values)
	data = data[request['variables']].sel({f"lat_{request['grid']}": slice(*request['lat']), f"lon_{request['grid']}": slice(*request['lon']), 'lead_time': slice(pd.Timestamp(request['start']) - init, pd.Timestamp(request['end']) - init + timedelta(hours=5))})
	data.load().to_netcdf(filename, format='NETCDF4', encoding={v: {'zlib': True, 'complevel': 4} for v in data.data_vars})


def _get_single_level(run, t0, end, fields, corners, billing_project):
	request = {
		'grid': '0p1',
		'variables': [_single_level_fields[f] for f in fields],
		'lat': [min([c[1] for c in corners]), max([c[1] for c in corners])],
		'lon': [min([c[0] % 360 for c in corners]), max([c[0] % 360 for c in corners])],
		'start': f'{t0:%Y-%m-%dT%H:%M:%S}',
		'end': f'{end:%Y-%m-%dT%H:%M:%S}'}
	data = _query(run, request, billing_project)
	data = data.stack(step=('lead_time', 'lead_subtime'), create_index=False)
	data = data.assign_coords(time=('step', data['init_time'].values + data['lead_time'].values + data['lead_subtime'].values))
	data = data.swap_dims({'step': 'time'}).sel(time=slice(t0, end))
	data = data.rename({'lat_0p1': 'lat', 'lon_0p1': 'lon', 'sample': 'member'})
	data = data.rename({v: f for f, v in _single_level_fields.items() if v in data.data_vars and v != f})
	data.attrs['init_time'] = f"{pd.Timestamp(data['init_time'].values):%Y-%m-%dT%H:%M:%S}"
	data = data.drop_vars(['lead_time', 'lead_subtime', 'datetime', 'init_time'], errors='ignore')
	return data[fields].transpose('member', 'time', 'lat', 'lon')


def _get_pressure_levels(run, t0, end, fields, levels, corners, billing_project):
	request = {
		'grid': '0p25',
		'variables': [_pressure_level_fields[f] for f in fields],
		'lat': [min([c[1] for c in corners]), max([c[1] for c in corners])],
		'lon': [min([c[0] % 360 for c in corners]), max([c[0] % 360 for c in corners])],
		'start': f'{t0:%Y-%m-%dT%H:%M:%S}',
		'end': f'{end:%Y-%m-%dT%H:%M:%S}'}
	data = _query(run, request, billing_project)
	data = data.assign_coords(time=('lead_time', data['init_time'].values + data['lead_time'].values))
	data = data.swap_dims({'lead_time': 'time'}).sel(time=slice(t0, end), level=levels)
	data = data.rename({'lat_0p25': 'lat', 'lon_0p25': 'lon', 'sample': 'member'})
	data = data.rename({v: f for f, v in _pressure_level_fields.items() if v in data.data_vars and v != f})
	data.attrs['init_time'] = f"{pd.Timestamp(data['init_time'].values):%Y-%m-%dT%H:%M:%S}"
	data = data.drop_vars(['lead_time', 'datetime', 'init_time'], errors='ignore')
	return data[fields].transpose('member', 'time', 'lat', 'lon', 'level')
