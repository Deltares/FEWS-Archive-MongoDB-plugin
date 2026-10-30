#  Copyright (c) 2026 INFISYS INC

import os
import json
import html
import logging
import argparse
import traceback
import xmltodict

from weathernext_3 import inputs, outputs
from weathernext_3.decorator import log
from datetime import datetime, timedelta

_parser = argparse.ArgumentParser()
_parser.add_argument('--config_path', '-c', dest='config_path', type=str, help='The fully qualified path to the configuration file', required=True)

_timestep = 1

_diag = '''<?xml version="1.0" encoding="UTF-8"?>
<Diag xmlns="http://www.wldelft.nl/fews/PI" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.wldelft.nl/fews/PI http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_diag.xsd" version="1.2">
	<line level="1" description="{description}" />
</Diag>'''


def main():
	args = _parser.parse_args()
	logging_format = '{"time": "%(asctime)s", "level": "%(levelname)s", "name": "%(name)s", "message": "%(message)s"}'
	logging.basicConfig(filename='weathernext_3.log', level=logging.INFO, filemode='w+', format=logging_format)
	stream_handler = logging.StreamHandler()
	stream_handler.setFormatter(logging.Formatter(logging_format))
	stream_handler.setLevel(logging.INFO)
	logging.getLogger().addHandler(stream_handler)
	run(args.config_path)


@log
def run(config_path):
	with open(config_path, 'r') as f:
		config = xmltodict.parse(f.read())['Run']
	properties = {c['@key']: c['@value'] for c in config['properties']['string']}

	output_diag_file = config['outputDiagnosticFile']
	t0 = datetime.strptime(f"{config['time0']['@date']}T{config['time0']['@time']}", "%Y-%m-%dT%H:%M:%S")

	t0_offset_timesteps = int(properties['time_zero_offset_timesteps'])
	predictions = json.loads(properties['forecast_length_timesteps'])
	fields = json.loads(properties['output_parameters'].replace('\'', '"'))
	corners = json.loads(properties['output_bounding_box'])
	output_folder = os.path.normpath(os.path.expanduser(os.path.expandvars(properties['output_folder'])))
	output_file = os.path.join(output_folder, properties['output_filename'])
	billing_project = properties['billing_project']

	try:
		t0 = t0 + timedelta(hours=t0_offset_timesteps * _timestep)
		single_level = inputs.read(t0, _timestep, predictions, fields, corners, billing_project)
		outputs.write(single_level, output_folder, output_file, t0)
	except Exception as e:
		with open(output_diag_file, 'w') as f:
			f.write(str.format(_diag, description=html.escape(traceback.format_exc())))
		raise e


if __name__ == '__main__':
	main()
