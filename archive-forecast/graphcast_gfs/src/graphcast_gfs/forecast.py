#  Copyright (c) 2026 INFISYS INC

import jax
import logging

import xarray as xr
import numpy as np
import haiku as hk
import pandas as pd

from datetime import timedelta, datetime
from graphcast import autoregressive, casting, data_utils, graphcast, normalization, rollout

from archive_forecast import solar
from archive_forecast.decorator import log

_logger = logging.getLogger('graphcast_gfs')
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


@log
def predict(model, single_level, pressure_levels, t0, timestep, predictions, diffs_stddev_by_level, mean_by_level, stddev_by_level, corners):
	observed_inputs = _get_inputs(single_level, pressure_levels)
	prediction_targets = _get_targets(observed_inputs, t0, timestep, predictions)
	prediction_forcings = _get_forcings(prediction_targets)

	run_forward_jit = _jit(model, stddev_by_level, mean_by_level, diffs_stddev_by_level)

	predictions = _predict(run_forward_jit, observed_inputs, prediction_targets, prediction_forcings, corners, t0, predictions)

	return xr.concat(predictions, dim='time')


@log
def _predict(run_forward_jit, observed_inputs, prediction_targets, prediction_forcings, corners, t0, predictions):
	clipped_predictions = []
	then = datetime.now()
	for step, prediction in enumerate(rollout.chunked_prediction_generator(run_forward_jit, rng=jax.random.PRNGKey(0), inputs=observed_inputs, targets_template=prediction_targets, forcings=prediction_forcings, num_steps_per_chunk=1), 1):
		clipped_predictions.append(_clip(_trim(jax.device_get(prediction)), corners))
		_logger.info(f'graphcast_gfs [t0: {t0} -> prediction {step}/{predictions}] complete in {datetime.now() - then}')
		then = datetime.now()
	return clipped_predictions


@log
def _jit(model, stddev_by_level, mean_by_level, diffs_stddev_by_level):
	state = {}
	return jax.jit(lambda rng, inputs, targets_template, forcings: _run_forward.apply(model.params, state, rng, model, stddev_by_level, mean_by_level, diffs_stddev_by_level, inputs, targets_template, forcings)[0])


@log
def _get_inputs(single_level, pressure_levels):
	inputs = xr.Dataset(
		{f: (('lat', 'lon'), single_level[f].isel(time=0).values) for f in ['geopotential_at_surface', 'land_sea_mask']} |
		{f: (('time', 'lat', 'lon', 'batch'), single_level[f].values[..., np.newaxis]) for f in ['mean_sea_level_pressure', '2m_temperature', '10m_u_component_of_wind', '10m_v_component_of_wind']} |
		{f: (('time', 'lat', 'lon', 'batch', 'level'), pressure_levels[f].transpose('time', 'lat', 'lon', 'level').values[:, :, :, np.newaxis, :]) for f in pressure_levels.data_vars},
		coords={'time': single_level['time'].values, 'lat': single_level['lat'].values, 'lon': single_level['lon'].values, 'level': pressure_levels['level'].values})
	return inputs.merge(_get_forcings(inputs))


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
	return xr.Dataset({f: (_get_dims(f), np.broadcast_to(np.float32(np.nan), [sizes[d] for d in _get_dims(f)])) for f in _prediction_fields}, coords=coords)


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
		'toa_incident_solar_radiation': solar.get_solar_radiation_cache(pd.DatetimeIndex(targets['time'].values).tolist())['toa_incident_solar_radiation'].values}
	return xr.Dataset({f: (_get_dims(f), v[..., np.newaxis]) for f, v in forcings.items()}, coords=targets.coords)


def _trim(x):
	return x[_prediction_fields].sortby('level', ascending=False).squeeze(['batch'])


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
