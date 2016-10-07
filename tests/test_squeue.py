"""Tests for SQueue"""
import os
import tempfile

import pytest

from squeue.squeue import Squeue


@pytest.fixture
def queue():
    queue_name = tempfile.NamedTemporaryFile().name
    yield Squeue(queue_name)
    os.unlink(queue_name)


def test_get_returns_message(queue):
    string_message = "foo"
    queue.put(string_message)
    assert queue.get() == string_message
