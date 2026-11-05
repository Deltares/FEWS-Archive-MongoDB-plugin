#  Copyright (c) 2026 INFISYS INC

import os

from datetime import datetime

import pytest

from archive_forecast import parameters

_run = '''<?xml version="1.0" encoding="UTF-8"?>
<Run xmlns="http://www.wldelft.nl/fews/PI" version="1.5">
	<timeZone>0.0</timeZone>
	<time0 date="2026-11-03" time="06:00:00"/>
	<outputDiagnosticFile>{diag}</outputDiagnosticFile>
	<properties>
		<string key="forecast_length_timesteps" value="56"/>
		<string key="time_zero_offset_timesteps" value="{offset}"/>
		<string key="output_bounding_box" value="[[-89.5, 38.1],[-81.0, 33.4]]"/>
		<string key="output_folder" value="{folder}"/>
		<string key="output_filename" value="Forecast.nc"/>
		<string key="downscale_method" value="cubic"/>
	</properties>
</Run>'''


@pytest.fixture
def run_file(tmp_path):
	def make(offset=1, folder=None):
		path = tmp_path / 'run.xml'
		path.write_text(_run.format(diag=tmp_path / 'diag.xml', offset=offset, folder=folder or tmp_path / 'output'))
		return str(path)
	return make


def test_get_parameters_reads_the_run_file(run_file, tmp_path):
	p = parameters.get_parameters(run_file(), 6)
	assert p.predictions == 56
	assert p.corners == [[-89.5, 38.1], [-81.0, 33.4]]
	assert p.output_file == os.path.join(str(tmp_path / 'output'), 'Forecast.nc')
	assert p.output_diag_file == str(tmp_path / 'diag.xml')
	assert p.properties['downscale_method'] == 'cubic'


@pytest.mark.parametrize('timestep, offset, expected', [(6, 1, datetime(2026, 11, 3, 12)), (1, 1, datetime(2026, 11, 3, 7)), (6, 0, datetime(2026, 11, 3, 6))])
def test_get_parameters_applies_time_zero_offset(run_file, timestep, offset, expected):
	assert parameters.get_parameters(run_file(offset=offset), timestep).t0 == expected


def test_get_parameters_expands_environment_variables_in_output_folder(run_file, tmp_path, monkeypatch):
	monkeypatch.setenv('FORECAST_OUTPUT', str(tmp_path / 'expanded'))
	p = parameters.get_parameters(run_file(folder='$FORECAST_OUTPUT'), 6)
	assert p.output_file == os.path.join(str(tmp_path / 'expanded'), 'Forecast.nc')


def test_parameters_are_read_only(run_file):
	p = parameters.get_parameters(run_file(), 6)
	with pytest.raises(AttributeError):
		p.predictions = 1
