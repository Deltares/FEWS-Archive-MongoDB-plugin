#  Copyright (c) 2024 INFISYS INC

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
from graphcast import autoregressive, casting, data_utils, graphcast, normalization, rollout

from decorator import log

logger = logging.getLogger('graphcast')

_prediction_fields = ['u_component_of_wind', 'v_component_of_wind', 'geopotential', 'specific_humidity', 'temperature', 'vertical_velocity', '10m_u_component_of_wind', '10m_v_component_of_wind', '2m_temperature', 'mean_sea_level_pressure', 'total_precipitation_6hr']

home_path = os.path.join(os.path.expanduser('~'), 'graphcast')
os.makedirs(home_path, exist_ok=True)


class Graphcast:
	@staticmethod
	@log
	def predict(model, single_level, pressure_levels, t0, timestep, predictions, model_path, dump_input=False):

		logger.info('diffs_stddev_by_level.nc load begin')
		with open(os.path.join(model_path, 'diffs_stddev_by_level.nc'), 'rb') as f:
			diffs_stddev_by_level = xr.load_dataset(f)
			logger.info('diffs_stddev_by_level.nc load complete')

		logger.info('mean_by_level.nc load begin')
		with open(os.path.join(model_path, 'mean_by_level.nc'), 'rb') as f:
			mean_by_level = xr.load_dataset(f)
			logger.info('mean_by_level.nc load complete')

		logger.info('stddev_by_level.nc load begin')
		with open(os.path.join(model_path, 'stddev_by_level.nc'), 'rb') as f:
			stddev_by_level = xr.load_dataset(f)
			logger.info('stddev_by_level.nc load complete')

		chunked_predictions = []
		px = 0
		for p in predictions:
			px += p

			logger.info(f'single_level [t0: {t0} -> predictions {px - p + 1}-{px}] Prediction._add_solar_radiation begin')
			single_level = Graphcast._add_solar_radiation(single_level)
			logger.info(f'single_level [t0: {t0} -> predictions {px - p + 1}-{px}] Prediction._add_solar_radiation complete')

			logger.info(f'prediction observed_values [t0: {t0} -> predictions {px - p + 1}-{px}] merge begin')
			observed_values = pd.merge(single_level, pressure_levels, left_index=True, right_index=True, how='inner', on=None, validate='one_to_many')
			logger.info(f'prediction observed_values [t0: {t0} -> predictions {px - p + 1}-{px}] merge complete')

			if dump_input:
				xr.Dataset.from_dataframe(observed_values).to_netcdf('inputs.nc', format='NETCDF4')

			logger.info(f'observed_inputs [t0: {t0} -> predictions {px - p + 1}-{px}] Prediction._add_progress begin')
			observed_inputs = Graphcast._add_progress(observed_values)
			logger.info(f'observed_inputs [t0: {t0} -> predictions {px - p + 1}-{px}] Prediction._add_progress complete')

			logger.info(f'observed_inputs [t0: {t0} -> predictions {px - p + 1}-{px}] Prediction._modify_coordinates begin')
			observed_inputs = Graphcast._modify_coordinates(observed_inputs.to_xarray())
			logger.info(f'observed_inputs [t0: {t0} -> predictions {px - p + 1}-{px}] Prediction._modify_coordinates complete')

			logger.info(f'prediction_targets [t0: {t0} -> predictions {px - p + 1}-{px}] Prediction._get_targets begin')
			prediction_targets = Graphcast._get_targets(observed_values, t0, timestep, p)
			logger.info(f'prediction_targets [t0: {t0} -> predictions {px - p + 1}-{px}] Prediction._get_targets complete')

			logger.info(f'prediction_targets [t0: {t0} -> predictions {px - p + 1}-{px}] Prediction._modify_coordinates begin')
			prediction_targets = Graphcast._modify_coordinates(prediction_targets.to_xarray())
			logger.info(f'prediction_targets [t0: {t0} -> predictions {px - p + 1}-{px}] Prediction._modify_coordinates complete')

			logger.info(f'prediction_forcings [t0: {t0} -> predictions {px - p + 1}-{px}] Prediction._add_progress begin')
			prediction_forcings = Graphcast._add_progress(prediction_targets.to_dataframe()[[]])
			logger.info(f'prediction_forcings [t0: {t0} -> predictions {px - p + 1}-{px}] Prediction._add_progress complete')

			logger.info(f'prediction_forcings [t0: {t0} -> predictions {px - p + 1}-{px}] Prediction._add_solar_radiation begin')
			prediction_forcings = Graphcast._add_solar_radiation(prediction_forcings)
			logger.info(f'prediction_forcings [t0: {t0} -> predictions {px - p + 1}-{px}] Prediction._add_solar_radiation complete')

			logger.info(f'prediction_forcings [t0: {t0} -> predictions {px - p + 1}-{px}] Prediction._modify_coordinates begin')
			prediction_forcings = Graphcast._modify_coordinates(prediction_forcings.to_xarray())
			logger.info(f'prediction_forcings [t0: {t0} -> predictions {px - p + 1}-{px}] Prediction._modify_coordinates complete')

			state = {}

			then = datetime.now()
			logger.info(f'graphcast [t0: {t0} -> predictions {px - p + 1}-{px}] rollout.chunked_prediction begin')
			run_forward_jit = jax.jit(lambda rng, inputs, targets_template, forcings: Graphcast._run_forward.apply(model.params, state, rng, model, stddev_by_level, mean_by_level, diffs_stddev_by_level, inputs, targets_template, forcings)[0])
			chunked_prediction = rollout.chunked_prediction(run_forward_jit, rng=jax.random.PRNGKey(0), inputs=observed_inputs, targets_template=prediction_targets, forcings=prediction_forcings)
			logger.info(f'graphcast [t0: {t0} -> predictions {px - p + 1}-{px}] rollout.chunked_prediction finished in {datetime.now() - then}')
			chunked_predictions.append(chunked_prediction)

			t0 = t0 + timedelta(hours=p*timestep)
			previous_two_timesteps = [t0 - timedelta(hours=1 * timestep), t0 - timedelta(hours=2 * timestep)]
			single_level = Graphcast._get_next_single_level(single_level, previous_two_timesteps, chunked_prediction)
			pressure_levels = Graphcast._get_next_pressure_levels(pressure_levels, previous_two_timesteps, chunked_prediction)

		return xr.concat(chunked_predictions, dim='time')

	@staticmethod
	def _get_next_single_level(single_level, previous_two_timesteps, chunked_prediction):
		existing_single_level = single_level[single_level.index.get_level_values('time').isin(previous_two_timesteps)]
		predicted_single_level = chunked_prediction[['10m_u_component_of_wind', '10m_v_component_of_wind', '2m_temperature', 'mean_sea_level_pressure']].to_dataframe()
		predicted_single_level = predicted_single_level[predicted_single_level.index.get_level_values('time').isin(previous_two_timesteps)]
		geopotential_at_surface_land_sea_mask = single_level[single_level.index.get_level_values('time') == single_level.index.get_level_values('time')[0]][['geopotential_at_surface', 'land_sea_mask']].reset_index('time', drop=True)
		predicted_single_level = pd.merge(predicted_single_level, geopotential_at_surface_land_sea_mask, left_index=True, right_index=True, how='inner', on=None, validate='many_to_one')
		return pd.concat([existing_single_level, predicted_single_level])

	@staticmethod
	def _get_next_pressure_levels(pressure_levels, previous_two_timesteps, chunked_prediction):
		existing_pressure_levels = pressure_levels[pressure_levels.index.get_level_values('time').isin(previous_two_timesteps)]
		predicted_pressure_levels = chunked_prediction[['u_component_of_wind', 'v_component_of_wind', 'geopotential', 'specific_humidity', 'temperature', 'vertical_velocity']].to_dataframe()
		predicted_pressure_levels = predicted_pressure_levels[predicted_pressure_levels.index.get_level_values('time').isin(previous_two_timesteps)]
		return pd.concat([existing_pressure_levels, predicted_pressure_levels])

	@staticmethod
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

	@staticmethod
	def _get_solar_radiation(longitude, latitude, dt):
		watts_to_joules = 3600
		altitude_degrees = get_altitude(latitude, longitude, dt.tz_localize("UTC"))
		return watts_to_joules * get_radiation_direct(dt, altitude_degrees) if altitude_degrees > 0 else 0

	@staticmethod
	def _get_solar_radiation_parallel(args):
		dt, lat = args
		longitudes = [lon / 4 for lon in range(0, 360 * 4, 1)]
		return [{'time': dt, 'lon': lon, 'lat': lat, 'toa_incident_solar_radiation': Graphcast._get_solar_radiation(lon, lat, dt)} for lon in longitudes]

	@staticmethod
	def _add_solar_radiation(data):
		if 'toa_incident_solar_radiation' not in data.columns:
			data['toa_incident_solar_radiation'] = np.nan
		dates = data[data['toa_incident_solar_radiation'].isna()].index.get_level_values('time').unique().tolist()
		if dates:
			solar_radiation = Graphcast._get_solar_radiation_cache(dates)
			data.update(solar_radiation)
		return data

	@staticmethod
	def _get_solar_radiation_cache(dates):
		cache = {}
		cache_dates = {pd.to_datetime(f.replace('solar_radiation_cache_', '').replace('.nc', '')): f for f in os.listdir(home_path) if f.startswith('solar_radiation_cache_') and f.endswith('.nc')}
		for date in dates:
			if date in cache_dates:
				try:
					ds = xr.load_dataset(os.path.join(home_path, cache_dates[date]))
				except:
					Graphcast._put_solar_radiation_cache(date)
					ds = xr.load_dataset(os.path.join(home_path, cache_dates[date]))
			else:
				Graphcast._put_solar_radiation_cache(date)
				ds = xr.load_dataset(os.path.join(home_path, f'solar_radiation_cache_{date:%Y%m%d%H%M}.nc'))

			cache[date] = ds.assign_coords(time=pd.to_datetime(ds.coords['time'].values)).to_dataframe()

		[os.remove(os.path.join(home_path, cache_dates[d])) for d in cache_dates if d < datetime.now() - timedelta(days=7)]
		return pd.concat(cache.values())

	@staticmethod
	def _put_solar_radiation_cache(date):
		latitudes = [lat / 4 for lat in range(-90 * 4, 90 * 4 + 1, 1)]
		with mp.Pool(mp.cpu_count()) as p:
			results = p.map(Graphcast._get_solar_radiation_parallel, [(date, lat) for lat in latitudes])
		pd.DataFrame([c for dt in results for c in dt]).set_index(keys=['time', 'lat', 'lon']).to_xarray().to_netcdf(os.path.join(home_path, f'solar_radiation_cache_{date:%Y%m%d%H%M}.nc'), format='NETCDF4')

	@staticmethod
	def _modify_coordinates(data):
		coordinates = {
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
		for v in data.data_vars:
			data[v] = data[v].isel({c: 0 for c in set(data[v].coords).difference(coordinates[v])})
		return data.drop_vars('batch')

	@staticmethod
	def _get_targets(observed_values, t0, timestep, predictions):
		lat, lon, level, batch = (sorted(observed_values.index.get_level_values(d).unique().tolist()) for d in ['lat', 'lon', 'level', 'batch'])
		time = [t0 + timedelta(hours=step * timestep) for step in range(predictions)]
		target = xr.Dataset({f: (['time', 'lat', 'lon', 'level'], np.full([len(time), len(lat), len(lon), len(level)], np.nan)) for f in _prediction_fields}, coords={'time': time, 'lat': lat, 'lon': lon, 'level': level, 'batch': batch})
		return target.to_dataframe()

	@staticmethod
	@hk.transform_with_state
	def _run_forward(model, stddev_by_level, mean_by_level, diffs_stddev_by_level, inputs, targets_template, forcings):
		predictor = graphcast.GraphCast(model.model_config, model.task_config)
		predictor = casting.Bfloat16Cast(predictor)
		predictor = normalization.InputsAndResiduals(predictor, stddev_by_level, mean_by_level, diffs_stddev_by_level)
		predictor = autoregressive.Predictor(predictor, gradient_checkpointing=True)
		return predictor(inputs, targets_template, forcings)
