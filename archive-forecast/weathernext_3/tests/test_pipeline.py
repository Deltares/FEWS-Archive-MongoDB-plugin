#  Copyright (c) 2026 INFISYS INC

import os
import sys

from datetime import datetime

import pytest

from weathernext_3 import pipeline


@pytest.fixture
def steps(monkeypatch):
	calls = {}

	def record(name, result=None):
		def step(*args):
			calls[name] = args
			return result
		return step

	monkeypatch.setattr(pipeline.inputs, 'read', record('inputs', ('single_level', 'percentiles')))
	monkeypatch.setattr(pipeline.outputs, 'write', record('write'))
	return calls


def test_run_passes_run_file_values_through_each_step(run_file, tmp_path, steps):
	pipeline.run(str(run_file))

	t0 = datetime(2026, 11, 3, 7)
	assert steps['inputs'] == (t0, 1, 3, ['temperature_2m', 'total_precipitation_1hr'], [[-89.5, 38.1], [-81.0, 33.4]])
	assert steps['write'] == ('single_level', 'percentiles', os.path.join(str(tmp_path / 'output'), 'Forecast.nc'), t0)
	assert not (tmp_path / 'diag.xml').exists()


def test_run_writes_diagnostics_on_failure(run_file, tmp_path, steps, monkeypatch):
	def fail(*args):
		raise PermissionError('quota exceeded')
	monkeypatch.setattr(pipeline.inputs, 'read', fail)

	with pytest.raises(PermissionError):
		pipeline.run(str(run_file))

	assert 'PermissionError: quota exceeded' in (tmp_path / 'diag.xml').read_text()


def test_main_passes_command_line_arguments_to_run(monkeypatch):
	calls = []
	monkeypatch.setattr(sys, 'argv', ['weathernext_3', '--config_path', 'run.xml'])
	monkeypatch.setattr(pipeline, 'configure', lambda name: calls.append(('configure', name)))
	monkeypatch.setattr(pipeline, 'run', lambda *args: calls.append(('run',) + args))

	pipeline.main()

	assert calls == [('configure', 'weathernext_3'), ('run', 'run.xml')]
