#  Copyright (c) 2026 INFISYS INC

import os
import jax
import logging

import xarray as xr
import multiprocessing as mp
import numpy as np
import haiku as hk
import pandas as pd

from pysolar.solar import get_altitude
from pysolar.radiation import get_radiation_direct
from datetime import timedelta, datetime
from weathernext.utils import autoregressive, casting, data_utils, normalization, rollout
from weathernext.weathernext1_graph import graphcast

from weathernext_ifs.decorator import log

_logger = logging.getLogger('weathernext_ifs')
_home_path = os.path.join(os.path.expanduser('~'), 'weathernext_ifs')
_prediction_fields = ['u_component_of_wind', 'v_component_of_wind', 'geopotential', 'specific_humidity', 'temperature', 'vertical_velocity', '10m_u_component_of_wind', '10m_v_component_of_wind', '2m_temperature', 'mean_sea_level_pressure', 'total_precipitation_6hr']
_coordinates = {
	'2m_temperature': {'batch', 'lon', 'lat', 'time'},
	'mean_sea_level_pressure': {'batch', 'lon', 'lat', 'time'},
	'10m_v_component_of_wind': {'batch', 'lon', 'lat', 'time'},
	'10m_u_component_of_wind': {'batch', 'lon', 'lat', 'time'},
	'total_precipitation_6hr': {'batch', 'lon', 'lat', 'time'},
	'temperature': {'batch', 'lon', 'lat', 'level', 'time'},
	'geopotential': {'batch', 'lon', 'lat', 'level', 'time'},
	'u_component_of_wind': {'batch', 'lon', 'lat', 'level', 'time'},
	'v_component_of_wind': {'batch', 'lon', 'lat', 'level', 'time'},
	'vertical_velocity': {'batch', 'lon', 'lat', 'level', 'time'},
	'specific_humidity': {'batch', 'lon', 'lat', 'level', 'time'},
	'toa_incident_solar_radiation': {'batch', 'lon', 'lat', 'time'},
	'year_progress_cos': {'batch', 'time'},
	'year_progress_sin': {'batch', 'time'},
	'day_progress_cos': {'batch', 'lon', 'time'},
	'day_progress_sin': {'batch', 'lon', 'time'},
	'geopotential_at_surface': {'lon', 'lat'},
	'land_sea_mask': {'lon', 'lat'}
}

os.makedirs(_home_path, exist_ok=True)


@log
def predict(model, single_level, pressure_levels, t0, timestep, predictions, diffs_stddev_by_level, mean_by_level, stddev_by_level, levels, fields, corners):
	chunked_predictions = []
	px = 0
	for p in predictions:
		px += p
		_logger.info(f'[t0: {t0} -> predictions {px - p + 1}-{px}]')

		single_level = _add_solar_radiation(single_level)
		observed_values = pd.merge(single_level, pressure_levels, left_index=True, right_index=True, how='inner', on=None, validate='one_to_many')

		observed_inputs = _add_progress(observed_values)
		observed_inputs = _modify_coordinates(observed_inputs.to_xarray())

		prediction_targets = _get_targets(observed_inputs, t0, timestep, p)
		prediction_forcings = _get_forcings(prediction_targets)

		state = {}

		then = datetime.now()
		_logger.info(f'weathernext_ifs [t0: {t0} -> predictions {px - p + 1}-{px}] rollout.chunked_prediction begin')
		run_forward_jit = jax.jit(lambda rng, inputs, targets_template, forcings: _run_forward.apply(model.params, state, rng, model, stddev_by_level, mean_by_level, diffs_stddev_by_level, inputs, targets_template, forcings)[0])
		chunked_prediction = rollout.chunked_prediction(run_forward_jit, rng=jax.random.PRNGKey(0), inputs=observed_inputs, targets_template=prediction_targets, forcings=prediction_forcings)
		_logger.info(f'weathernext_ifs [t0: {t0} -> predictions {px - p + 1}-{px}] rollout.chunked_prediction finished in {datetime.now() - then}')
		chunked_predictions.append(_clip(_trim(chunked_prediction, levels, fields), corners))

		t0 = t0 + timedelta(hours=p*timestep)
		previous_two_timesteps = [t0 - timedelta(hours=1 * timestep), t0 - timedelta(hours=2 * timestep)]
		single_level = _get_next_single_level(single_level, previous_two_timesteps, chunked_prediction)
		pressure_levels = _get_next_pressure_levels(pressure_levels, previous_two_timesteps, chunked_prediction)

	return xr.concat(chunked_predictions, dim='time')


def _get_next_single_level(single_level, previous_two_timesteps, chunked_prediction):
	existing_single_level = single_level[single_level.index.get_level_values('time').isin(previous_two_timesteps)]
	predicted_single_level = chunked_prediction[['10m_u_component_of_wind', '10m_v_component_of_wind', '2m_temperature', 'mean_sea_level_pressure']].to_dataframe()
	predicted_single_level = predicted_single_level[predicted_single_level.index.get_level_values('time').isin(previous_two_timesteps)]
	geopotential_at_surface_land_sea_mask = single_level[single_level.index.get_level_values('time') == single_level.index.get_level_values('time')[0]][['geopotential_at_surface', 'land_sea_mask']].reset_index('time', drop=True)
	predicted_single_level = pd.merge(predicted_single_level, geopotential_at_surface_land_sea_mask, left_index=True, right_index=True, how='inner', on=None, validate='many_to_one')
	return pd.concat([existing_single_level, predicted_single_level])


def _get_next_pressure_levels(pressure_levels, previous_two_timesteps, chunked_prediction):
	existing_pressure_levels = pressure_levels[pressure_levels.index.get_level_values('time').isin(previous_two_timesteps)]
	predicted_pressure_levels = chunked_prediction[['u_component_of_wind', 'v_component_of_wind', 'geopotential', 'specific_humidity', 'temperature', 'vertical_velocity']].to_dataframe()
	predicted_pressure_levels = predicted_pressure_levels[predicted_pressure_levels.index.get_level_values('time').isin(previous_two_timesteps)]
	return pd.concat([existing_pressure_levels, predicted_pressure_levels])


@log
def _add_progress(data):
	day_progress = []
	for dt in data.index.get_level_values('time').unique():
		mask = data.index.get_level_values('time') == dt

		progress = data_utils.get_year_progress(dt.timestamp())
		data.loc[mask, 'year_progress_sin'] = np.sin(2 * np.pi * progress)
		data.loc[mask, 'year_progress_cos'] = np.cos(2 * np.pi * progress)

		longitudes = data.index.get_level_values('lon').unique()
		progress = data_utils.get_day_progress(dt.timestamp(), np.array(longitudes))
		progress = pd.DataFrame.from_dict({(lon, dt): [sin, cos] for lon, sin, cos in zip(longitudes, np.sin(progress * 2 * np.pi), np.cos(progress * 2 * np.pi))}, orient='index', columns=['day_progress_sin', 'day_progress_cos'])
		progress.index = pd.MultiIndex.from_tuples(progress.index, names=['lon', 'time'])
		day_progress.append(data.loc[mask].join(progress, on=['lon', 'time']))
	return pd.concat(day_progress)


def _get_solar_radiation(longitude, latitude, dt):
	watts_to_joules = 3600
	altitude_degrees = get_altitude(latitude, longitude, dt.tz_localize("UTC"))
	return watts_to_joules * get_radiation_direct(dt, altitude_degrees) if altitude_degrees > 0 else 0


def _get_solar_radiation_parallel(args):
	dt, lat = args
	return np.array([_get_solar_radiation(lon / 4, lat, dt) for lon in range(0, 360 * 4, 1)], dtype='float64')


@log
def _add_solar_radiation(data):
	if 'toa_incident_solar_radiation' not in data.columns:
		data['toa_incident_solar_radiation'] = np.nan
	dates = data[data['toa_incident_solar_radiation'].isna()].index.get_level_values('time').unique().tolist()
	if dates:
		solar_radiation = _get_solar_radiation_cache(dates)
		extra = [n for n in data.index.names if n not in solar_radiation.index.names]
		data = data.reset_index(level=extra)
		data.update(solar_radiation)
		data = data.set_index(extra, append=True)
	return data


def _get_solar_radiation_cache(dates):
	cache = {}
	cache_dates = {pd.to_datetime(f.replace('solar_radiation_cache_', '').replace('.nc', '')): f for f in os.listdir(_home_path) if f.startswith('solar_radiation_cache_') and f.endswith('.nc')}
	for date in dates:
		if date in cache_dates:
			try:
				ds = xr.load_dataset(os.path.join(_home_path, cache_dates[date]))
			except:
				_put_solar_radiation_cache(date)
				ds = xr.load_dataset(os.path.join(_home_path, cache_dates[date]))
		else:
			_put_solar_radiation_cache(date)
			ds = xr.load_dataset(os.path.join(_home_path, f'solar_radiation_cache_{date:%Y%m%d%H%M}.nc'))

		cache[date] = ds.assign_coords(time=pd.to_datetime(ds.coords['time'].values)).to_dataframe()

	[os.remove(os.path.join(_home_path, cache_dates[d])) for d in cache_dates if d < min(dates) - timedelta(days=7)]
	return pd.concat(cache.values())


def _put_solar_radiation_cache(date):
	latitudes = [lat / 4 for lat in range(-90 * 4, 90 * 4 + 1, 1)]
	with mp.Pool(mp.cpu_count()) as p:
		results = p.map(_get_solar_radiation_parallel, [(date, lat) for lat in latitudes])
	solar_radiation_cache = xr.Dataset(
		{'toa_incident_solar_radiation': (('time', 'lat', 'lon'), np.stack(results)[np.newaxis])},
		coords={'time': [date], 'lat': latitudes, 'lon': [lon / 4 for lon in range(0, 360 * 4, 1)]})
	solar_radiation_cache.to_netcdf(os.path.join(_home_path, f'solar_radiation_cache_{date:%Y%m%d%H%M}.nc'), format='NETCDF4', encoding={v: {'zlib': True, 'complevel': 4} for v in solar_radiation_cache.data_vars})


@log
def _modify_coordinates(data):
	for v in data.data_vars:
		data[v] = data[v].isel({c: 0 for c in set(data[v].coords).difference(_coordinates[v])})
	return data.drop_vars('batch')


def _get_dims(field):
	return tuple(d for d in ['time', 'lat', 'lon', 'level', 'batch'] if d in _coordinates[field])


@log
def _get_targets(observed_inputs, t0, timestep, predictions):
	coords = {
		'time': np.array([t0 + timedelta(hours=step * timestep) for step in range(predictions)], dtype='datetime64[us]'),
		'lat': observed_inputs['lat'].values,
		'lon': observed_inputs['lon'].values,
		'level': observed_inputs['level'].values}
	sizes = {d: len(c) for d, c in coords.items()} | {'batch': observed_inputs.sizes['batch']}
	return xr.Dataset({f: (_get_dims(f), np.full([sizes[d] for d in _get_dims(f)], np.nan)) for f in _prediction_fields}, coords=coords)


@log
def _get_forcings(targets):
	seconds = targets['time'].values.astype('datetime64[s]').astype(np.int64)
	year_progress = data_utils.get_year_progress(seconds)
	day_progress = data_utils.get_day_progress(seconds, targets['lon'].values)
	forcings = {
		'year_progress_sin': np.sin(2 * np.pi * year_progress),
		'year_progress_cos': np.cos(2 * np.pi * year_progress),
		'day_progress_sin': np.sin(2 * np.pi * day_progress),
		'day_progress_cos': np.cos(2 * np.pi * day_progress),
		'toa_incident_solar_radiation': _get_solar_radiation_cache(pd.DatetimeIndex(targets['time'].values).tolist()).to_xarray()['toa_incident_solar_radiation'].values}
	return xr.Dataset({f: (_get_dims(f), v[..., np.newaxis]) for f, v in forcings.items()}, coords=targets.coords)


@log
def _trim(x, levels, fields):
	return x[fields].sel(level=levels).squeeze(['batch'])


@log
def _clip(x, corners):
	return x.where(
		(x.lon >= min([c[0] % 360 for c in corners])) &
		(x.lon <= max([c[0] % 360 for c in corners])) &
		(x.lat >= min([c[1] for c in corners])) &
		(x.lat <= max([c[1] for c in corners])), drop=True)


@hk.transform_with_state
def _run_forward(model, stddev_by_level, mean_by_level, diffs_stddev_by_level, inputs, targets_template, forcings):
	predictor = graphcast.GraphCast(model.model_config, model.task_config)
	predictor = casting.Bfloat16Cast(predictor)
	predictor = normalization.InputsAndResiduals(predictor, stddev_by_level, mean_by_level, diffs_stddev_by_level)
	predictor = autoregressive.Predictor(predictor, gradient_checkpointing=True)
	return predictor(inputs, targets_template, forcings)
