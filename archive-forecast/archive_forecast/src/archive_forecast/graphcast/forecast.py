#  Copyright (c) 2026 INFISYS INC

import xarray as xr
import numpy as np

from datetime import timedelta
from archive_forecast.log import log

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
def get_inputs(single_level, pressure_levels):
	return xr.Dataset(
		{f: (('lat', 'lon'), single_level[f].isel(time=0).values) for f in ['geopotential_at_surface', 'land_sea_mask']} |
		{f: (('time', 'lat', 'lon', 'batch'), single_level[f].values[..., np.newaxis]) for f in ['mean_sea_level_pressure', '2m_temperature', '10m_u_component_of_wind', '10m_v_component_of_wind']} |
		{f: (('time', 'lat', 'lon', 'batch', 'level'), pressure_levels[f].transpose('time', 'lat', 'lon', 'level').values[:, :, :, np.newaxis, :]) for f in pressure_levels.data_vars},
		coords={'time': single_level['time'].values, 'lat': single_level['lat'].values, 'lon': single_level['lon'].values, 'level': pressure_levels['level'].values})


def get_dims(field):
	return tuple(d for d in ['time', 'lat', 'lon', 'level', 'batch'] if d in _coordinates[field])


@log
def get_targets(observed_inputs, t0, timestep, predictions):
	coords = {
		'time': np.array([t0 + timedelta(hours=step * timestep) for step in range(predictions)], dtype='datetime64[us]'),
		'lat': observed_inputs['lat'].values,
		'lon': observed_inputs['lon'].values,
		'level': observed_inputs['level'].values}
	sizes = {d: len(c) for d, c in coords.items()} | {'batch': observed_inputs.sizes['batch']}
	return xr.Dataset({f: (get_dims(f), np.broadcast_to(np.float32(np.nan), [sizes[d] for d in get_dims(f)])) for f in _prediction_fields}, coords=coords)


def trim(x):
	return x[_prediction_fields].sortby('level', ascending=False).squeeze(['batch'])
