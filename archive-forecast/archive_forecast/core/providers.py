#  Copyright (c) 2026 INFISYS INC

import os
import logging
import importlib

logger = logging.getLogger('archive_forecast')

# Provider name -> 'module:Class'. Modules are imported only when selected; providers must not be loaded side by side.
_providers = {
	'graphcast_gfs': 'archive_forecast.sources.gfs:Gfs',
	'weathernext_ifs': 'archive_forecast.sources.ifs:Ifs',
}

# Fallback for run files without a 'provider' property.
_model_folders = {
	'GraphCastOperationalGfs': 'graphcast_gfs',
	'GraphCastOperationalIfs': 'weathernext_ifs',
}

_required = ['timestep', 'forecast']


class Providers:
	@staticmethod
	def name(properties, model_path):
		name = properties.get('provider') or _model_folders.get(os.path.basename(model_path))
		if name is None:
			raise ValueError(f"no 'provider' property and unknown model_path: {os.path.basename(model_path)} [{', '.join(_model_folders)}]")
		return name

	@staticmethod
	def load(name):
		if name not in _providers:
			raise ValueError(f"provider: {name} [{', '.join(_providers)}]")
		module, cls = _providers[name].split(':')
		logger.info(f'provider {name} load begin [{module}.{cls}]')
		provider = getattr(importlib.import_module(module), cls)
		missing = [r for r in _required if not hasattr(provider, r)]
		if missing:
			raise TypeError(f'provider: {name} missing {missing}')
		logger.info(f'provider {name} load complete')
		return provider
