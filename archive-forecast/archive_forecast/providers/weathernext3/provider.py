#  Copyright (c) 2026 INFISYS INC

import logging

import numpy as np
import pandas as pd
import xarray as xr

from datetime import timedelta

from archive_forecast.core import provider
from archive_forecast.core.decorator import log
from archive_forecast.providers.weathernext3 import template

logger = logging.getLogger('archive_forecast')

_gravity = 9.80665
_scale_height_m = 7400


# Retrieves pre-executed forecasts: read only; model and map are pass-through because data arrives in template form.
class WeatherNext3(provider.Provider):
	template = template
	timestep = 1
	members = 4

	# STUB: synthetic ensemble members until WeatherNext 3 retrieval is implemented; assets_path will hold the retrieval credentials.
	@staticmethod
	@log
	def read(request):
		logger.warning('WeatherNext3 is a stub: returning synthetic ensemble members')
		return WeatherNext3._synthetic(request.t0, request.steps)

	@staticmethod
	def _synthetic(t0, steps, grid=1.0):
		levels = np.array(template.levels, dtype=float)
		height = _scale_height_m * np.log(1013.25 / levels)
		# (mean, member spread); pressure-level means vary by level as in a standard atmosphere.
		base = {
			'2m_temperature': (288.0, 3.0),
			'mean_sea_level_pressure': (101325.0, 500.0),
			'10m_u_component_of_wind': (2.0, 3.0),
			'10m_v_component_of_wind': (0.0, 3.0),
			'total_precipitation_1hr': (0.2, 0.5),
			'temperature': (np.maximum(288.15 - 0.0065 * height, 216.65), 2.0),
			'geopotential': (_gravity * height, 50.0),
			'u_component_of_wind': (5.0 + height / 1000, 4.0),
			'v_component_of_wind': (0.0, 4.0),
			'vertical_velocity': (0.0, 0.2),
			'specific_humidity': (0.01 * np.exp(-height / 2500), 0.0005),
		}
		coords = {
			'member': np.arange(WeatherNext3.members),
			'time': [t0 + timedelta(hours=step * WeatherNext3.timestep) for step in range(steps)],
			'lat': np.arange(-90, 90 + grid, grid),
			'lon': np.arange(0, 360, grid),
			'level': template.levels,
		}
		rng = np.random.default_rng(int(pd.Timestamp(t0).timestamp()))
		data = {}
		for declared in template.products.values():
			dims = declared['dims']
			shape = [len(coords[d]) for d in dims]
			for f in declared['fields']:
				mean, spread = base[f]
				data[f] = (dims, (mean + spread * rng.standard_normal(shape)).astype(np.float32))
		return xr.Dataset(data, coords=coords)
