#  Copyright (c) 2024 INFISYS INC

import logging
import argparse
import traceback

from datetime import timedelta

from archive_forecast.core import provider as providers
from archive_forecast.core.decorator import log
from archive_forecast.core.fews import Fews, Request
from archive_forecast.core.validation import Validate


def main():
	parser = argparse.ArgumentParser()
	parser.add_argument('--config_path', '-c', dest='config_path', type=str, help='The fully qualified path to the FEWS PI run file', required=True)
	parser.add_argument('--provider', '-p', dest='provider', type=str, help=f'The provider to run, one of [{", ".join(providers.available())}]; defaults to the run file property "provider"')
	parser.add_argument('--assets_path', '--model_path', '-a', '-m', dest='assets_path', type=str, help="The fully qualified path to the provider's assets (weights, credentials)")
	args = parser.parse_args()

	logging_format = '{"time": "%(asctime)s", "level": "%(levelname)s", "name": "%(name)s", "message": "%(message)s"}'
	logging.basicConfig(filename='archive_forecast.log', level=logging.INFO, filemode='w+', format=logging_format)
	stream_handler = logging.StreamHandler()
	stream_handler.setFormatter(logging.Formatter(logging_format))
	stream_handler.setLevel(logging.INFO)
	logging.getLogger().addHandler(stream_handler)

	Pipeline(args.config_path, args.provider, args.assets_path).run()


class Pipeline:
	"""read (provider) -> model (provider, optional) -> map to template (provider) -> write for FEWS (common)."""

	def __init__(self, config_path, provider, assets_path):
		self.run_file = Fews.read_run(config_path)
		self.diag_file = self.run_file['outputDiagnosticFile']
		self.provider = provider
		self.assets_path = assets_path

	@log
	def run(self):
		try:
			request = Request(self.run_file, self.assets_path)
			name = Pipeline._name(self.provider, request.provider)
			provider = providers.load(name)
			request.t0 = request.time0 + timedelta(hours=request.t0_offset_timesteps * provider.timestep)
			Validate.request(name, provider.template, request)

			data = provider.read(request)
			data = provider.model(data, request)
			data = provider.map(data, request)
			Validate.template(name, provider.template, data, request)

			Fews.write(data, provider.template, request)

		except Exception as e:
			Fews.write_diag(self.diag_file, traceback.format_exc())
			raise e

	@staticmethod
	def _name(argument, property):
		if argument and property and argument != property:
			raise ValueError(f"--provider {argument} != run file provider {property}")
		if not (argument or property):
			raise ValueError("no provider: pass --provider or set the run file property 'provider'")
		return argument or property
