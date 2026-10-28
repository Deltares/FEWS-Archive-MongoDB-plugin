#  Copyright (c) 2026 INFISYS INC

import os

import xarray as xr

from weathernext.weathernext1_graph import graphcast
from weathernext.utils import checkpoint
from weathernext_ifs.decorator import log


@log
def read(model_path):
	with open(os.path.join(model_path, 'weights.npz'), 'rb') as f:
		model_weights = checkpoint.load(f, graphcast.CheckPoint)
	diffs_stddev_by_level, mean_by_level, stddev_by_level = (xr.load_dataset(os.path.join(model_path, f)) for f in ['diffs_stddev_by_level.nc', 'mean_by_level.nc', 'stddev_by_level.nc'])
	return model_weights, diffs_stddev_by_level, mean_by_level, stddev_by_level
