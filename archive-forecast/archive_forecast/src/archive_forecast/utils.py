#  Copyright (c) 2026 INFISYS INC

from archive_forecast.log import log


@log
def zero_negative(x, fields):
	for f in fields:
		if f in x:
			x[f] = x[f].where(x[f] >= 0, 0)
	return x
