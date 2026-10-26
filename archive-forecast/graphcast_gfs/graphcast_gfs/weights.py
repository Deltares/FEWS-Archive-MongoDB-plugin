#  Copyright (c) 2026 INFISYS INC

import os
import logging

import xarray as xr

from graphcast import graphcast, checkpoint
from graphcast_gfs.decorator import log

logger = logging.getLogger('graphcast_gfs')


@log
def read(model_path):
	logger.info('model load begin')
	with open(os.path.join(model_path, 'weights.npz'), 'rb') as f:
		model_weights = checkpoint.load(f, graphcast.CheckPoint)
	logger.info('model load complete')

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

	return model_weights, diffs_stddev_by_level, mean_by_level, stddev_by_level
