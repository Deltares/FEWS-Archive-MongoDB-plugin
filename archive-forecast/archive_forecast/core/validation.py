#  Copyright (c) 2026 INFISYS INC

import logging

logger = logging.getLogger('archive_forecast')


class ValidationError(ValueError):
	pass


# Generic checks. A provider's template is its own expectation; nothing here assumes a field, dimension or domain.
class Validate:
	@staticmethod
	def request(name, template, request):
		"""Before reading: can this template satisfy the run file, and is the template itself complete?"""
		dims = template.dims
		provided = Validate._fields_of(template)
		problems = []
		if 'time' not in dims:
			problems.append("no 'time' dimension role")
		Validate._missing(problems, 'output_parameters', request.fields, provided)
		Validate._missing(problems, 'template variables', request.fields, template.variables)
		Validate._missing(problems, 'template fields for non_negative', template.non_negative, provided)
		if request.levels is not None:
			if 'z' in dims:
				Validate._missing(problems, 'output_levels', request.levels, template.levels)
			else:
				problems.append("output_levels given but no 'z' dimension role")
		if (request.corners is not None or request.downscale is not None) and not {'x', 'y'} <= set(dims):
			problems.append("output_bounding_box/downscaled_grid_cell_size_deg given but no 'x'/'y' dimension roles")
		if request.downscale is not None and request.downscale_method is None:
			problems.append('downscaled_grid_cell_size_deg given without downscale_method')
		if 'member' in dims and not hasattr(template, 'ensemble'):
			problems.append("'member' dimension role without template ensemble products")
		Validate._raise(name, 'request', problems)

	@staticmethod
	def template(name, template, x, request):
		"""After mapping: does the data match the template it claims?"""
		dims = template.dims
		problems = []
		Validate._fields(problems, x.data_vars, Validate._fields_of(template))
		for product, declared in template.products.items():
			for f in declared['fields']:
				if f in x.data_vars and set(x[f].dims) != set(declared['dims']):
					problems.append(f'{f} dims {sorted(x[f].dims)} != {product} {sorted(declared["dims"])}')
		if dims.get('z') in x.dims:
			Validate._values(problems, dims['z'], x[dims['z']].values, template.levels)
		if x.sizes.get(dims['time']) != request.steps:
			problems.append(f"{dims['time']} steps {x.sizes.get(dims['time'])} != {request.steps}")
		Validate._raise(name, 'template', problems)

	@staticmethod
	def table(name, x, dims, fields, coords=None):
		"""A provider's tabular data (pandas, indexed by dims): exact index, exact columns, expected coordinate values, no nulls."""
		problems = []
		if set(x.index.names) != set(dims):
			problems.append(f'index {sorted(x.index.names)} != {sorted(dims)}')
		Validate._fields(problems, x.columns, fields)
		for coord, expected in (coords or {}).items():
			if coord in x.index.names:
				Validate._values(problems, coord, x.index.get_level_values(coord).unique(), expected)
		nulls = [c for c in x.columns if x[c].isna().any()]
		if nulls:
			problems.append(f'null values {nulls}')
		Validate._raise(name, 'table', problems)

	@staticmethod
	def _fields_of(template):
		return [f for declared in template.products.values() for f in declared['fields']]

	@staticmethod
	def _fields(problems, found, expected):
		Validate._missing(problems, 'fields', expected, found)
		extra = sorted(set(found) - set(expected))
		if extra:
			problems.append(f'undeclared fields {extra}')

	@staticmethod
	def _values(problems, coord, found, expected):
		if set(found) != set(expected):
			problems.append(f'{coord} {sorted(str(v) for v in found)} != {sorted(str(v) for v in expected)}')

	@staticmethod
	def _missing(problems, what, required, available):
		missing = [r for r in required if r not in set(available)]
		if missing:
			problems.append(f'missing {what} {missing}')

	@staticmethod
	def _raise(name, what, problems):
		if problems:
			raise ValidationError(f"{name} {what}: {'; '.join(problems)}")
		logger.info(f'{name} {what} valid')
