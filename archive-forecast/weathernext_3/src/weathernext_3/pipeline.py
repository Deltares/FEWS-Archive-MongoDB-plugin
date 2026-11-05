#  Copyright (c) 2026 INFISYS INC

import json
import argparse

from weathernext_3 import inputs, outputs
from archive_forecast import parameters, diag
from archive_forecast.log import log, configure

_parser = argparse.ArgumentParser()
_parser.add_argument('--config_path', '-c', dest='config_path', type=str, help='The fully qualified path to the configuration file', required=True)

_timestep = 1


def main():
	args = _parser.parse_args()
	configure('weathernext_3')
	run(args.config_path)


@log
def run(config_path):
	config = parameters.get_parameters(config_path, _timestep)
	fields = json.loads(config.properties['output_parameters'].replace('\'', '"'))

	with diag.diagnostics(config.output_diag_file):
		single_level, percentiles = inputs.read(config.t0, _timestep, config.predictions, fields, config.corners)
		outputs.write(single_level, percentiles, config.output_file, config.t0)
