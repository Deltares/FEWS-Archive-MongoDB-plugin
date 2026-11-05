#  Copyright (c) 2026 INFISYS INC

from datetime import datetime

import numpy as np
import xarray as xr

from archive_forecast import netcdf


def test_write_names_file_by_kind_and_t0_and_creates_folder(tmp_path):
	x = xr.Dataset({'a': (('lat',), np.arange(3.0))}, coords={'lat': [0.0, 1.0, 2.0]})
	output_file = tmp_path / 'out' / 'Forecast.nc'

	netcdf.write(x, str(output_file), 'SingleLevel', datetime(2026, 11, 3, 12))

	written = tmp_path / 'out' / 'Forecast.SingleLevel.202611031200.nc'
	assert written.exists()
	y = xr.load_dataset(written)
	assert y['a'].identical(x['a'])
	assert y['a'].encoding['zlib'] is True
	assert y['a'].encoding['complevel'] == 4
