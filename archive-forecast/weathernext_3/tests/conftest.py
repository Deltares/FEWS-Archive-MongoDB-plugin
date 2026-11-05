#  Copyright (c) 2026 INFISYS INC

import pytest

_run = '''<?xml version="1.0" encoding="UTF-8"?>
<Run xmlns="http://www.wldelft.nl/fews/PI" version="1.5">
	<timeZone>0.0</timeZone>
	<time0 date="2026-11-03" time="06:00:00"/>
	<outputDiagnosticFile>{diag}</outputDiagnosticFile>
	<properties>
		<string key="forecast_length_timesteps" value="3"/>
		<string key="time_zero_offset_timesteps" value="1"/>
		<string key="output_parameters" value="['temperature_2m','total_precipitation_1hr']"/>
		<string key="output_bounding_box" value="[[-89.5, 38.1],[-81.0, 33.4]]"/>
		<string key="output_folder" value="{folder}"/>
		<string key="output_filename" value="Forecast.nc"/>
	</properties>
</Run>'''


@pytest.fixture
def run_file(tmp_path):
	path = tmp_path / 'run.xml'
	path.write_text(_run.format(diag=tmp_path / 'diag.xml', folder=tmp_path / 'output'))
	return path
