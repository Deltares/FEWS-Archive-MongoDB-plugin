#  Copyright (c) 2026 INFISYS INC

import xmltodict

import pytest

from archive_forecast import diag


def test_diagnostics_writes_escaped_traceback_and_reraises(tmp_path):
	diag_file = tmp_path / 'diag.xml'

	with pytest.raises(ValueError, match='boom'):
		with diag.diagnostics(str(diag_file)):
			raise ValueError('boom <&>')

	line = xmltodict.parse(diag_file.read_text())['Diag']['line']
	assert line['@level'] == '1'
	assert 'ValueError: boom <&>' in line['@description']


def test_diagnostics_writes_nothing_on_success(tmp_path):
	diag_file = tmp_path / 'diag.xml'

	with diag.diagnostics(str(diag_file)):
		pass

	assert not diag_file.exists()
