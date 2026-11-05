#  Copyright (c) 2026 INFISYS INC

import os
import argparse

from graphcast_gfs import weights, inputs, outputs, forecast
from archive_forecast import fews
from archive_forecast.log import log, configure

_parser = argparse.ArgumentParser()
_parser.add_argument('--config_path', '-c', dest='config_path', type=str, help='The fully qualified path to the configuration file', required=True)
_parser.add_argument('--weights_path', '-w', dest='weights_path', type=str, help='The fully qualified path to the weights folder', required=True)

_timestep = 6
_observed_timesteps = 2


def main():
	args = _parser.parse_args()
	configure('graphcast_gfs')
	run(args.config_path, args.weights_path)


@log
def run(config_path, weights_path):
	config = fews.read_run(config_path, _timestep)
	weights_path = os.path.normpath(os.path.expanduser(os.path.expandvars(weights_path)))
	downscale = float(config.properties['downscaled_grid_cell_size_deg'])
	downscale_method = config.properties['downscale_method']

	with fews.diagnostics(config.output_diag_file):
		model_weights, diffs_stddev_by_level, mean_by_level, stddev_by_level = weights.read(weights_path)
		single_level, pressure_levels = inputs.read(config.t0, _timestep, _observed_timesteps)
		model_forecast = forecast.predict(model_weights, single_level, pressure_levels, config.t0, _timestep, config.predictions, diffs_stddev_by_level, mean_by_level, stddev_by_level, config.corners)
		outputs.write(model_forecast, config.output_folder, config.output_file, downscale, downscale_method, config.t0)


if __name__ == '__main__':
	main()
