#  Copyright (c) 2026 INFISYS INC

import jax
import logging

import xarray as xr
import numpy as np
import haiku as hk
import pandas as pd

from datetime import datetime
from graphcast import autoregressive, casting, data_utils, graphcast, normalization, rollout

from archive_forecast.graphcast import common, solar, utils
from archive_forecast.log import log

_logger = logging.getLogger('graphcast_gfs')


@log
def predict(model, single_level, pressure_levels, t0, timestep, predictions, diffs_stddev_by_level, mean_by_level, stddev_by_level, corners):
	observed_inputs = common.get_inputs(single_level, pressure_levels)
	observed_inputs = observed_inputs.merge(_get_forcings(observed_inputs))
	prediction_targets = common.get_targets(observed_inputs, t0, timestep, predictions)
	prediction_forcings = _get_forcings(prediction_targets)

	run_forward_jit = _jit(model, stddev_by_level, mean_by_level, diffs_stddev_by_level)

	predictions = _predict(run_forward_jit, observed_inputs, prediction_targets, prediction_forcings, corners, t0, predictions)

	return xr.concat(predictions, dim='time')


@log
def _predict(run_forward_jit, observed_inputs, prediction_targets, prediction_forcings, corners, t0, predictions):
	clipped_predictions = []
	then = datetime.now()
	for step, prediction in enumerate(rollout.chunked_prediction_generator(run_forward_jit, rng=jax.random.PRNGKey(0), inputs=observed_inputs, targets_template=prediction_targets, forcings=prediction_forcings, num_steps_per_chunk=1), 1):
		clipped_predictions.append(utils.clip(common.trim(jax.device_get(prediction)), corners))
		_logger.info(f'graphcast_gfs [t0: {t0} -> prediction {step}/{predictions}] complete in {datetime.now() - then}')
		then = datetime.now()
	return clipped_predictions


@log
def _jit(model, stddev_by_level, mean_by_level, diffs_stddev_by_level):
	state = {}
	return jax.jit(lambda rng, inputs, targets_template, forcings: _run_forward.apply(model.params, state, rng, model, stddev_by_level, mean_by_level, diffs_stddev_by_level, inputs, targets_template, forcings)[0])


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
	return xr.Dataset({f: (common.get_dims(f), v[..., np.newaxis]) for f, v in forcings.items()}, coords=targets.coords)


@hk.transform_with_state
def _run_forward(model, stddev_by_level, mean_by_level, diffs_stddev_by_level, inputs, targets_template, forcings):
	predictor = graphcast.GraphCast(model.model_config, model.task_config)
	predictor = casting.Bfloat16Cast(predictor)
	predictor = normalization.InputsAndResiduals(predictor, stddev_by_level, mean_by_level, diffs_stddev_by_level)
	predictor = autoregressive.Predictor(predictor, gradient_checkpointing=True)
	return predictor(inputs, targets_template, forcings)
