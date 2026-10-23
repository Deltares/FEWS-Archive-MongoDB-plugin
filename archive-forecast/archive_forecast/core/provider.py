#  Copyright (c) 2026 INFISYS INC

import logging
import importlib

logger = logging.getLogger('archive_forecast')

# Provider name -> 'module:Class'. Only the selected provider is imported: providers' dependencies may not coexist in one process.
_providers = {
	'graphcast_gfs': 'archive_forecast.providers.graphcast_gfs.provider:Gfs',
	'weathernext_ifs': 'archive_forecast.providers.weathernext_ifs.provider:Ifs',
	'weathernext3': 'archive_forecast.providers.weathernext3.provider:WeatherNext3',
}


class Provider:
	"""A data provider as pipeline stages: read (specific) -> model (specific, optional) -> map (specific -> template).

	template is the module holding the FEWS format agreed with the FEWS stewards; timestep is in hours.
	model and map default to pass-through: a provider that retrieves pre-executed data in template form only reads.
	"""
	template = None
	timestep = None

	@staticmethod
	def read(request):
		raise NotImplementedError

	@staticmethod
	def model(data, request):
		return data

	@staticmethod
	def map(data, request):
		return data


def available():
	return sorted(_providers)


def load(name):
	if name not in _providers:
		raise ValueError(f"provider: {name} [{', '.join(available())}]")
	module, cls = _providers[name].split(':')
	logger.info(f'provider {name} load begin [{module}.{cls}]')
	provider = getattr(importlib.import_module(module), cls)
	logger.info(f'provider {name} load complete')
	return provider
