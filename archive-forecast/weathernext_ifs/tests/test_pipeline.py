#  Copyright (c) 2026 INFISYS INC

import os
import sys

from datetime import datetime

import pytest

from weathernext_ifs import pipeline


@pytest.fixture
def steps(monkeypatch):
	calls = {}

	def record(name, result=None):
		def step(*args):
			calls[name] = args
			return result
		return step

	monkeypatch.setattr(pipeline.weights, 'read', record('weights', ('model', 'diffs', 'mean', 'stddev')))
	monkeypatch.setattr(pipeline.inputs, 'read', record('inputs', ('single_level', 'pressure_levels')))
	monkeypatch.setattr(pipeline.forecast, 'predict', record('predict', 'forecast'))
	monkeypatch.setattr(pipeline.outputs, 'write', record('write'))
	return calls


def test_run_passes_run_file_values_through_each_step(run_file, tmp_path, steps):
	pipeline.run(str(run_file), str(tmp_path / 'weights'))

	t0 = datetime(2026, 11, 3, 12)
	assert steps['weights'] == (str(tmp_path / 'weights'),)
	assert steps['inputs'] == (t0, 6, 2)
	assert steps['predict'] == ('model', 'single_level', 'pressure_levels', t0, 6, 4, 'diffs', 'mean', 'stddev', [[-89.5, 38.1], [-81.0, 33.4]])
	assert steps['write'] == ('forecast', os.path.join(str(tmp_path / 'output'), 'Forecast.nc'), 0.0625, 'cubic', t0)
	assert not (tmp_path / 'diag.xml').exists()


def test_run_writes_diagnostics_on_failure(run_file, tmp_path, steps, monkeypatch):
	def fail(*args):
		raise ConnectionError('no data')
	monkeypatch.setattr(pipeline.inputs, 'read', fail)

	with pytest.raises(ConnectionError):
		pipeline.run(str(run_file), str(tmp_path / 'weights'))

	assert 'ConnectionError: no data' in (tmp_path / 'diag.xml').read_text()


def test_main_passes_command_line_arguments_to_run(monkeypatch):
	calls = []
	monkeypatch.setattr(sys, 'argv', ['weathernext_ifs', '--config_path', 'run.xml', '--weights_path', 'weights'])
	monkeypatch.setattr(pipeline, 'configure', lambda name: calls.append(('configure', name)))
	monkeypatch.setattr(pipeline, 'run', lambda *args: calls.append(('run',) + args))

	pipeline.main()

	assert calls == [('configure', 'weathernext_ifs'), ('run', 'run.xml', 'weights')]
