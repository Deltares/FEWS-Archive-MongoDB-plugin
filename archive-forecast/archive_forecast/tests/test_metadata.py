#  Copyright (c) 2026 INFISYS INC

import numpy as np
import xarray as xr

from archive_forecast import metadata


def _dataset():
	return xr.Dataset(
		{'2m_temperature': (('lat', 'lon'), np.zeros((2, 2))), 'unknown_field': (('lat', 'lon'), np.zeros((2, 2)))},
		coords={'lat': [0.0, 1.0], 'lon': [0.0, 1.0]})


def test_apply_sets_variable_and_coordinate_attributes():
	x = metadata.apply(_dataset())
	assert x['2m_temperature'].attrs['units'] == 'K'
	assert x['2m_temperature'].attrs['abbreviation'] == 'TMP'
	assert x['lat'].attrs['standard_name'] == 'latitude'
	assert x['lon'].attrs['units'] == 'degrees_east'


def test_apply_sets_global_attributes():
	x = metadata.apply(_dataset())
	assert x.attrs['Conventions'] == 'CF-1.6'
	assert x.attrs['originating_or_generating_center'] == 'Tennessee Valley Authority (TVA)'


def test_apply_leaves_unknown_variables_untouched():
	x = metadata.apply(_dataset())
	assert x['unknown_field'].attrs == {}


def test_every_variable_has_units_and_long_name():
	for name, attributes in metadata._output_metadata['variables'].items():
		assert 'units' in attributes, name
		assert 'long_name' in attributes, name
