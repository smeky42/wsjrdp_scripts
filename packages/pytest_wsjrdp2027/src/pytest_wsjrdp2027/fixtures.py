from __future__ import annotations

import unittest.mock as _unittest_mock

import pytest as _pytest


@_pytest.fixture
def mock_execute_query_fetchall_dicts(monkeypatch):
    """Patch ``wsjrdp2027._pg._execute_query_fetchall_dicts`` with a ``Mock``.

    The returned ``Mock`` replaces the low-level executor so query-building
    code can run without a database. Set its ``return_value`` to control the
    rows handed back to the caller.
    """
    from wsjrdp2027 import _pg

    mock = _unittest_mock.Mock()
    monkeypatch.setattr(_pg, "_execute_query_fetchall_dicts", mock)
    return mock


@_pytest.fixture
def forbid_to_connection(monkeypatch):
    """Patch ``wsjrdp2027._pg.to_connection`` to raise on any call.

    Use as a guard to prove that a unit test never opens a database
    connection: any call fails with an ``AssertionError``.
    """
    from wsjrdp2027 import _pg

    def _raise(*args, **kwargs):
        raise AssertionError("to_connection must not be called in unit tests")

    monkeypatch.setattr(_pg, "to_connection", _raise)
    return _raise
