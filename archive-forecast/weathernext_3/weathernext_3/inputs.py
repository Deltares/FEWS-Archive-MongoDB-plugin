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

logger = logging.getLogger('weathernext_3')

home_path = os.path.join(os.path.expanduser('~'), 'weathernext_3')
os.makedirs(home_path, exist_ok=True)

_bucket = 'weathernext3_spatial'
_runs_path = 'weathernext_3_0_0/zarr/2026_to_present'
_search_hours = 48

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

	logger.info('_get_run begin')
	run = _get_run(t0 - timedelta(hours=timestep), end, bool(pressure_level_fields), billing_project)
	logger.info(f'_get_run complete [{run}]')

	logger.info('_get_single_level download begin')
	single_level = _get_single_level(run, t0, end, single_level_fields, corners, billing_project) if single_level_fields else None
	logger.info('_get_single_level download complete')

	logger.info('_get_pressure_levels download begin')
	pressure_levels = _get_pressure_levels(run, t0, end, pressure_level_fields, levels, corners, billing_project) if pressure_level_fields else None
	logger.info('_get_pressure_levels download complete')

	return single_level, pressure_levels


# latest complete run initialized at or before latest that covers the whole forecast; pressure levels exist only on 6 hourly runs
@retry
def _get_run(latest, end, pressure_levels, billing_project):
	store = _store(None, billing_project)
	runs = {}
	for run in obstore.list_with_delimiter(store, _runs_path)['common_prefixes']:
		name = run.rstrip('/').split('/')[-1]
		runs[run.rstrip('/')] = datetime.strptime(name[:11], '%Y%m%d_%H')

	for run, init in sorted(runs.items(), key=lambda r: (r[1], r[0]), reverse=True):
		horizon = 360 if init.hour % 6 == 0 else 48
		if init > latest or init < latest - timedelta(hours=_search_hours) or init + timedelta(hours=horizon) < end or (pressure_levels and init.hour % 6):
			continue
		try:
			obstore.head(store, f'{run}/success')
		except FileNotFoundError:
			logger.info(f'_get_run incomplete [{run}]')
			continue
		return run

	raise LookupError(f'No complete run initialized between {latest - timedelta(hours=_search_hours)} and {latest} covers {end}')


@retry
def _query(name, request, billing_project):
	query = json.dumps({name: request})
	filename = os.path.join(home_path, f"{hashlib.sha3_256(query.encode()).hexdigest()}.nc")
	logger.info(f'_query begin [{query} -> {filename}]')
	cache_dates = {datetime.fromtimestamp(os.path.getctime(os.path.join(home_path, f)), tz=timezone.utc): f for f in os.listdir(home_path)}
	if os.path.exists(filename):
		try:
			data = xr.load_dataset(filename)
		except:
			_put_data_cache(name, request, billing_project, filename)
			data = xr.load_dataset(filename)
	else:
		_put_data_cache(name, request, billing_project, filename)
		data = xr.load_dataset(filename)

	if not data['time'].size:
		raise ImportError("Empty response dataset")

	[os.remove(os.path.join(home_path, cache_dates[d])) for d in cache_dates if d < datetime.now(tz=timezone.utc) - timedelta(days=7)]

	logger.info('_query complete')

	return data


def _put_data_cache(name, request, billing_project, filename):
	data = xr.open_zarr(zarr.storage.ObjectStore(_store(f'{name}/predictions.zarr', billing_project), read_only=True), chunks={})
	init = pd.Timestamp(data['init_time'].values)
	lat, lon = f"lat_{request['grid']}", f"lon_{request['grid']}"

	data = data[request['variables']].sel({lat: slice(*request['lat']), lon: slice(*request['lon'])})
	if 'levels' in request:
		data = data.sel(level=request['levels'])
	data = data.drop_vars(['datetime', 'init_time'], errors='ignore')

	# surface variables carry hourly lead_subtime offsets (-5h..0h) within each 6 hourly lead_time
	if 'lead_subtime' in data.dims:
		data = data.stack(step=('lead_time', 'lead_subtime'), create_index=False)
		data = data.assign_coords(time=('step', (init + data['lead_time'].values + data['lead_subtime'].values)))
		data = data.swap_dims({'step': 'time'}).drop_vars(['step', 'lead_time', 'lead_subtime'], errors='ignore')
	else:
		data = data.assign_coords(time=('lead_time', (init + data['lead_time'].values)))
		data = data.swap_dims({'lead_time': 'time'}).drop_vars(['lead_time'])

	data = data.sel(time=slice(request['start'], request['end']))
	data = data.rename({lat: 'lat', lon: 'lon', 'sample': 'member'})
	data.attrs['init_time'] = f'{init:%Y-%m-%dT%H:%M:%S}'
	data.load().to_netcdf(filename, format='NETCDF4', encoding={v: {'zlib': True, 'complevel': 4} for v in data.data_vars})


def _get_single_level(run, t0, end, fields, corners, billing_project):
	request = {
		'grid': '0p1',
		'variables': [_single_level_fields[f] for f in fields],
		'lat': _lat(corners),
		'lon': _lon(corners),
		'start': f'{t0:%Y-%m-%dT%H:%M:%S}',
		'end': f'{end:%Y-%m-%dT%H:%M:%S}'}
	data = _query(run, request, billing_project)
	data = data.rename({v: f for f, v in _single_level_fields.items() if v in data.data_vars and v != f})
	return data[fields].transpose('member', 'time', 'lat', 'lon')


def _get_pressure_levels(run, t0, end, fields, levels, corners, billing_project):
	request = {
		'grid': '0p25',
		'variables': [_pressure_level_fields[f] for f in fields],
		'levels': levels,
		'lat': _lat(corners),
		'lon': _lon(corners),
		'start': f'{t0:%Y-%m-%dT%H:%M:%S}',
		'end': f'{end:%Y-%m-%dT%H:%M:%S}'}
	data = _query(run, request, billing_project)
	data = data.rename({v: f for f, v in _pressure_level_fields.items() if v in data.data_vars and v != f})
	return data[fields].transpose('member', 'time', 'lat', 'lon', 'level')


def _lat(corners):
	return [min([c[1] for c in corners]), max([c[1] for c in corners])]


def _lon(corners):
	return [min([c[0] % 360 for c in corners]), max([c[0] % 360 for c in corners])]


def _store(prefix, billing_project):
	return GCSStore(_bucket, prefix=prefix, client_options={'default_headers': {'x-goog-user-project': billing_project}})
