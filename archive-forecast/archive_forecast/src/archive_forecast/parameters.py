#  Copyright (c) 2026 INFISYS INC

import os
import json
import xmltodict

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True)
class Parameters:
	t0: datetime
	predictions: int
	corners: list
	output_file: str
	output_diag_file: str
	properties: dict


def get_parameters(config_path, timestep):
	with open(config_path, 'r') as f:
		config = xmltodict.parse(f.read())['Run']
	properties = {c['@key']: c['@value'] for c in config['properties']['string']}

	t0 = datetime.strptime(f"{config['time0']['@date']}T{config['time0']['@time']}", "%Y-%m-%dT%H:%M:%S")
	t0_offset_timesteps = int(properties['time_zero_offset_timesteps'])
	output_folder = os.path.normpath(os.path.expanduser(os.path.expandvars(properties['output_folder'])))

	return Parameters(
		t0=t0 + timedelta(hours=t0_offset_timesteps * timestep),
		predictions=int(properties['forecast_length_timesteps']),
		corners=json.loads(properties['output_bounding_box']),
		output_file=os.path.join(output_folder, properties['output_filename']),
		output_diag_file=config['outputDiagnosticFile'],
		properties=properties)
