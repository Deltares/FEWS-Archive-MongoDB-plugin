#  Copyright (c) 2026 INFISYS INC

import os
import json
import html
import traceback
import xmltodict

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta

_diag = '''<?xml version="1.0" encoding="UTF-8"?>
<Diag xmlns="http://www.wldelft.nl/fews/PI" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.wldelft.nl/fews/PI http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_diag.xsd" version="1.2">
	<line level="1" description="{description}" />
</Diag>'''


@dataclass(frozen=True)
class Run:
	t0: datetime
	predictions: int
	corners: list
	output_folder: str
	output_file: str
	output_diag_file: str
	properties: dict


def read_run(config_path, timestep):
	with open(config_path, 'r') as f:
		config = xmltodict.parse(f.read())['Run']
	properties = {c['@key']: c['@value'] for c in config['properties']['string']}

	t0 = datetime.strptime(f"{config['time0']['@date']}T{config['time0']['@time']}", "%Y-%m-%dT%H:%M:%S")
	t0_offset_timesteps = int(properties['time_zero_offset_timesteps'])
	output_folder = os.path.normpath(os.path.expanduser(os.path.expandvars(properties['output_folder'])))

	return Run(
		t0=t0 + timedelta(hours=t0_offset_timesteps * timestep),
		predictions=int(properties['forecast_length_timesteps']),
		corners=json.loads(properties['output_bounding_box']),
		output_folder=output_folder,
		output_file=os.path.join(output_folder, properties['output_filename']),
		output_diag_file=config['outputDiagnosticFile'],
		properties=properties)


@contextmanager
def diagnostics(output_diag_file):
	try:
		yield
	except Exception:
		with open(output_diag_file, 'w') as f:
			f.write(str.format(_diag, description=html.escape(traceback.format_exc())))
		raise
