#  Copyright (c) 2026 INFISYS INC

import json
import logging

import pytest

from archive_forecast.log import configure, log


def _function():
	return 42


def _failure():
	raise ValueError('boom')


def test_log_returns_result_and_logs_begin_and_complete(caplog):
	caplog.set_level(logging.INFO)
	assert log(_function)() == 42
	messages = [r.getMessage() for r in caplog.records]
	assert messages[0] == '_function begin'
	assert messages[1].startswith('_function complete in ')


def test_log_uses_the_top_level_package_logger(caplog):
	caplog.set_level(logging.INFO)
	_function.__module__ = 'some_model.inputs'
	try:
		log(_function)()
	finally:
		_function.__module__ = __name__
	assert {r.name for r in caplog.records} == {'some_model'}


def test_log_logs_traceback_and_reraises(caplog):
	caplog.set_level(logging.INFO)
	with pytest.raises(ValueError, match='boom'):
		log(_failure)()
	errors = [r for r in caplog.records if r.levelno == logging.ERROR]
	assert len(errors) == 1
	assert 'ValueError: boom' in errors[0].getMessage()


def test_log_keeps_function_name():
	assert log(_function).__name__ == '_function'


def test_configure_writes_json_lines_to_named_log_file_and_console(tmp_path, monkeypatch):
	monkeypatch.chdir(tmp_path)
	monkeypatch.setattr(logging.root, 'handlers', [])
	monkeypatch.setattr(logging.root, 'level', logging.root.level)
	try:
		configure('some_model')
		logging.getLogger('some_model').info('hello')
		handlers = list(logging.root.handlers)
	finally:
		for handler in logging.root.handlers:
			handler.close()

	assert sorted(type(h).__name__ for h in handlers) == ['FileHandler', 'StreamHandler']
	line = json.loads((tmp_path / 'some_model.log').read_text().splitlines()[0])
	assert (line['level'], line['name'], line['message']) == ('INFO', 'some_model', 'hello')
