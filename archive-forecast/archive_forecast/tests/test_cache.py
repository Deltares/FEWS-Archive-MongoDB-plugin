#  Copyright (c) 2026 INFISYS INC

import os
import time

import pytest

from archive_forecast import cache


def test_folder_creates_hidden_folder_in_home(tmp_path, monkeypatch):
	monkeypatch.setattr(os.path, 'expanduser', lambda p: p.replace('~', str(tmp_path)))
	path = cache.folder('model')
	assert path == os.path.join(str(tmp_path), '.model')
	assert os.path.isdir(path)


def test_get_downloads_on_miss_then_hits(tmp_path):
	calls = []

	def put(filename):
		calls.append(filename)
		with open(filename, 'w') as f:
			f.write('data')

	def load(filename):
		with open(filename) as f:
			return f.read()

	assert cache.get(str(tmp_path), 'key', 'txt', load, put) == 'data'
	assert cache.get(str(tmp_path), 'key', 'txt', load, put) == 'data'
	assert len(calls) == 1
	assert calls[0].endswith('.txt')


def test_get_rebuilds_unreadable_file_once(tmp_path):
	calls = []

	def put(filename):
		calls.append(filename)
		with open(filename, 'w') as f:
			f.write('good')

	def load(filename):
		with open(filename) as f:
			text = f.read()
		if text != 'good':
			raise ValueError('corrupt')
		return text

	cache.get(str(tmp_path), 'key', 'txt', load, put)
	with open(calls[0], 'w') as f:
		f.write('bad')

	assert cache.get(str(tmp_path), 'key', 'txt', load, put) == 'good'
	assert len(calls) == 2


def test_get_raises_when_download_stays_unreadable(tmp_path):
	def put(filename):
		with open(filename, 'w') as f:
			f.write('bad')

	def load(filename):
		raise ValueError('corrupt')

	with pytest.raises(ValueError):
		cache.get(str(tmp_path), 'key', 'txt', load, put)


def test_get_uses_different_files_for_different_keys(tmp_path):
	def put(filename):
		with open(filename, 'w') as f:
			f.write(os.path.basename(filename))

	def load(filename):
		with open(filename) as f:
			return f.read()

	assert cache.get(str(tmp_path), 'a', 'txt', load, put) != cache.get(str(tmp_path), 'b', 'txt', load, put)


def test_clean_removes_only_files_older_than_seven_days(tmp_path, monkeypatch):
	old, new = tmp_path / 'old.grib2', tmp_path / 'new.grib2'
	old.write_text('x')
	new.write_text('x')
	ages = {str(old): time.time() - 8 * 86400, str(new): time.time() - 1 * 86400}
	monkeypatch.setattr(os.path, 'getctime', lambda p: ages[str(p)])

	cache.clean(str(tmp_path))

	assert not old.exists()
	assert new.exists()
