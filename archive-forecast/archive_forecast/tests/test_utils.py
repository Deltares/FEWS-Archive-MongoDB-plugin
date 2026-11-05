#  Copyright (c) 2026 INFISYS INC

import numpy as np
import xarray as xr

from archive_forecast import utils


def test_zero_negative_zeroes_listed_fields_only():
	x = xr.Dataset({'precipitation': ('n', [-1.0, 0.0, 2.0, np.nan]), 'temperature': ('n', [-5.0, 1.0, 2.0, 3.0])})
	y = utils.zero_negative(x.copy(deep=True), ['precipitation'])
	np.testing.assert_array_equal(y['precipitation'].values, [0.0, 0.0, 2.0, 0.0])
	np.testing.assert_array_equal(y['temperature'].values, [-5.0, 1.0, 2.0, 3.0])


def test_zero_negative_skips_missing_fields():
	x = xr.Dataset({'precipitation': ('n', [-1.0])})
	y = utils.zero_negative(x, ['precipitation', 'not_present'])
	assert list(y.data_vars) == ['precipitation']
