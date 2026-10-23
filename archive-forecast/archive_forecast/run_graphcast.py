#  Copyright (c) 2024 INFISYS INC

import os
import json
import logging
import argparse
import traceback
import xmltodict

from archive_forecast.decorator import log
from archive_forecast.core.fews import Fews
from datetime import datetime, timedelta

_parser = argparse.ArgumentParser()
_parser.add_argument('--config_path', '-c', dest='config_path', type=str, help='The fully qualified path to the configuration file', required=True)
_parser.add_argument('--model_path', '-m', dest='model_path', type=str, help='The fully qualified path to the model folder', required=True)
_args = _parser.parse_args()

_timestep = 6
_observed_timesteps = 2


def main():
	logging_format = '{"time": "%(asctime)s", "level": "%(levelname)s", "name": "%(name)s", "message": "%(message)s"}'
	logging.basicConfig(filename='archive_forecast.log', level=logging.INFO, filemode='w+', format=logging_format)
	stream_handler = logging.StreamHandler()
	stream_handler.setFormatter(logging.Formatter(logging_format))
	stream_handler.setLevel(logging.INFO)
	logging.getLogger().addHandler(stream_handler)
	RunGraphcast().main()


class RunGraphcast:

	def __init__(self):
		with open(_args.config_path, 'r') as f:
			config = xmltodict.parse(f.read())['Run']
		properties = {c['@key']: c['@value'] for c in config['properties']['string']}

		self.output_diag_file = config['outputDiagnosticFile']
		self.t0 = datetime.strptime(f"{config['time0']['@date']}T{config['time0']['@time']}", "%Y-%m-%dT%H:%M:%S")

		self.t0_offset_timesteps = int(properties['time_zero_offset_timesteps'])
		self.predictions = json.loads(properties['forecast_length_timesteps'])
		self.levels = json.loads(properties['output_levels'])
		self.fields = json.loads(properties['output_parameters'].replace('\'', '"'))
		self.corners = json.loads(properties['output_bounding_box'])
		self.model_path = os.path.normpath(os.path.expanduser(os.path.expandvars(_args.model_path)))
		self.output_folder = os.path.normpath(os.path.expanduser(os.path.expandvars(properties['output_folder'])))
		self.output_file = os.path.join(self.output_folder, properties['output_filename'])
		self.downscale = float(properties['downscaled_grid_cell_size_deg'])
		self.downscale_method = properties['downscale_method']

	@log
	def main(self):

		try:
			t0 = self.t0 + timedelta(hours=self.t0_offset_timesteps * _timestep)
			model_name = os.path.basename(self.model_path)
			if model_name == 'GraphCastOperationalIfs':
				from archive_forecast.sources.ifs import Ifs as source
			elif model_name == 'GraphCastOperationalGfs':
				from archive_forecast.sources.gfs import Gfs as source
			else:
				raise ValueError(f'model_path: {model_name}')
			results = source.model(t0, self.predictions, self.model_path, _timestep, _observed_timesteps)

			single_level_results, pressure_levels_results = Fews.format(results, self.levels, self.fields, self.corners, self.downscale, self.downscale_method)
			Fews.save(single_level_results, pressure_levels_results, self.output_file, t0)

		except Exception as e:
			Fews.write_diag(self.output_diag_file, traceback.format_exc())
			raise e


if __name__ == '__main__':
	main()
