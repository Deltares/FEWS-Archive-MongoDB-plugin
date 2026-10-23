#  Copyright (c) 2026 INFISYS INC

import os
import json
import html
import xmltodict

import numpy as np
import pandas as pd

from datetime import datetime
from archive_forecast.core.decorator import log

_diag = '''<?xml version="1.0" encoding="UTF-8"?>
<Diag xmlns="http://www.wldelft.nl/fews/PI" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.wldelft.nl/fews/PI http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_diag.xsd" version="1.2">
	<line level="1" description="{description}" />
</Diag>'''


def _path(p):
	return os.path.normpath(os.path.expanduser(os.path.expandvars(p)))


class Request:
	"""What FEWS asked for, from the PI run file. t0 is set by the pipeline once the provider's timestep is known."""

	def __init__(self, run, assets_path):
		properties = {c['@key']: c['@value'] for c in run['properties']['string']}

		self.time0 = datetime.strptime(f"{run['time0']['@date']}T{run['time0']['@time']}", "%Y-%m-%dT%H:%M:%S")
		self.t0 = None
		self.t0_offset_timesteps = int(properties['time_zero_offset_timesteps'])
		self.predictions = json.loads(properties['forecast_length_timesteps'])
		self.steps = sum(self.predictions)
		self.fields = json.loads(properties['output_parameters'].replace('\'', '"'))
		# Optional: only meaningful for templates with 'z' and 'x'/'y' dimension roles (checked by Validate.request).
		self.levels = json.loads(properties['output_levels']) if 'output_levels' in properties else None
		self.corners = json.loads(properties['output_bounding_box']) if 'output_bounding_box' in properties else None
		self.downscale = float(properties['downscaled_grid_cell_size_deg']) if 'downscaled_grid_cell_size_deg' in properties else None
		self.downscale_method = properties.get('downscale_method')
		self.output_file = os.path.join(_path(properties['output_folder']), properties['output_filename'])
		self.provider = properties.get('provider')
		self.assets_path = _path(assets_path) if assets_path else None


# The common end of the pipeline: everything here works from the provider's template (dimension roles, products,
# levels, constraints, attributes), never from field or dimension names.
class Fews:
	@staticmethod
	def read_run(file):
		with open(file, 'r') as f:
			return xmltodict.parse(f.read())['Run']

	@staticmethod
	def write_diag(file, description):
		with open(file, 'w') as f:
			f.write(str.format(_diag, description=html.escape(description)))

	@staticmethod
	@log
	def write(x, template, request):
		dims = template.dims
		x = Fews._trim(x, request.fields, dims.get('z'), request.levels)
		if request.corners is not None:
			x = Fews._clip(x, dims['x'], dims['y'], request.corners)
		if request.downscale is not None:
			x = Fews._resample(x, dims['x'], dims['y'], request.downscale, request.downscale_method)
		x = Fews._zero_negative(x, [v for v in template.non_negative if v in x.data_vars])
		x = Fews._update_datetime(x, dims['time'])
		if 'member' in dims:
			outputs = {template.ensemble['members']: x, template.ensemble['mean']: Fews._ensemble_mean(x, dims['member'])}
		else:
			outputs = {None: x}

		os.makedirs(os.path.dirname(request.output_file), exist_ok=True)
		output_file_parts = request.output_file.split('.')
		for suffix, output in outputs.items():
			for product, declared in template.products.items():
				present = [v for v in output.data_vars if v in declared['fields']]
				if present:
					name = f'{product}.{suffix}' if suffix else product
					result = Fews._metadata(output[present], template.variables, template.global_attributes)
					Fews._save(result, f"{'.'.join(output_file_parts[:-1])}.{name}.{request.t0:%Y%m%d%H%M}.{output_file_parts[-1]}")

	@staticmethod
	@log
	def _update_datetime(x, time):
		return x.assign_coords({time: pd.to_datetime(x.coords[time])})

	@staticmethod
	@log
	def _zero_negative(x, non_negative):
		for v in non_negative:
			x[v] = x[v].where(x[v] >= 0, 0)
		return x

	@staticmethod
	@log
	def _ensemble_mean(x, member):
		mean = x.mean(member, keep_attrs=True)
		for v in mean.data_vars:
			mean[v].attrs['cell_methods'] = f'{member}: mean'
		return mean

	@staticmethod
	@log
	def _save(x, file):
		output_format = file.split('.')[-1]
		if output_format in ['nc', 'netcdf']:
			x.to_netcdf(file, format='NETCDF4')
		elif output_format == 'zarr':
			x.to_zarr(file, mode='w')
		elif output_format == 'json':
			with open(file, 'w') as w:
				json.dump(x.to_dict(), w, default=str)
		elif output_format in ['txt', 'csv']:
			x.to_dataframe().to_csv(file)

	@staticmethod
	@log
	def _metadata(x, variables, global_attributes):
		for r in x.variables:
			if r in variables:
				for k, v in variables[r].items():
					x[r].attrs[k] = v
		for k, v in global_attributes.items():
			x.attrs[k] = v
		return x

	@staticmethod
	@log
	def _trim(x, fields, z, levels):
		x = x[fields]
		return x.sel({z: levels}) if levels is not None and z in x.dims else x

	@staticmethod
	@log
	def _clip(x, lon, lat, corners):
		# Corners are converted to the data's own longitude convention; the data itself is never rewritten.
		convert = (lambda c: c + 360 if c < 0 else c) if x[lon].values.max() > 180 else (lambda c: c - 360 if c > 180 else c)
		min_lon = convert(min([c[0] for c in corners]))
		max_lon = convert(max([c[0] for c in corners]))

		return x.where((x[lon] >= min_lon) & (x[lon] <= max_lon) & (x[lat] >= min([c[1] for c in corners])) & (x[lat] <= max([c[1] for c in corners])), drop=True)

	@staticmethod
	@log
	def _resample(x, lon, lat, downscale, downscale_method):
		lat_min, lat_max = x[lat].values.min(), x[lat].values.max()
		lon_min, lon_max = x[lon].values.min(), x[lon].values.max()
		return x.interp({lat: np.arange(lat_min, lat_max, downscale), lon: np.arange(lon_min, lon_max, downscale)}, downscale_method)
