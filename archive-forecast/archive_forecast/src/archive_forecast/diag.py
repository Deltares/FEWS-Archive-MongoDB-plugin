#  Copyright (c) 2026 INFISYS INC

import html
import traceback

from contextlib import contextmanager

_diag = '''<?xml version="1.0" encoding="UTF-8"?>
<Diag xmlns="http://www.wldelft.nl/fews/PI" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.wldelft.nl/fews/PI http://fews.wldelft.nl/schemas/version1.0/pi-schemas/pi_diag.xsd" version="1.2">
	<line level="1" description="{description}" />
</Diag>'''


@contextmanager
def diagnostics(output_diag_file):
	try:
		yield
	except Exception:
		with open(output_diag_file, 'w') as f:
			f.write(str.format(_diag, description=html.escape(traceback.format_exc())))
		raise
