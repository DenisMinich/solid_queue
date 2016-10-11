# pylint: disable=invalid-name
# pylint: disable=missing-docstring
# pylint: disable=redefined-outer-name
"""Tests for SQueue"""
import multiprocessing
import os
import tempfile

import pytest

from squeue.squeue import Squeue


def put_data_to_queue(test_queue_name, data_queue):
    Squeue(test_queue_name).put(data_queue.get())


def get_data_from_queue(test_queue_name, data_queue):
    data_queue.put(Squeue(test_queue_name).get())


@pytest.fixture
def queue():
    queue_name = tempfile.NamedTemporaryFile().name
    yield Squeue(queue_name)
    os.unlink(queue_name)


def test_get_returns_message(queue):
    string_message = "foo"
    queue.put(string_message)
    assert queue.get() == string_message


def test_get_works_after_put_to_the_same_queue(queue):
    first_message = "foo"
    second_message = "bar"
    queue.put(first_message)
    queue.put(second_message)
    assert queue.get() == first_message
    assert queue.get() == second_message


def test_get_returns_message_in_right_sequence(queue):
    first_message = "foo"
    second_message = "bar"
    second_queue = Squeue(queue.name)
    queue.put(first_message)
    assert second_queue.get() == first_message
    second_queue.put(second_message)
    assert queue.get() == second_message


def test_put_not_changes_read_position(queue):
    first_message = "spam"
    second_message = "ham"
    third_message = "eggs"
    second_queue = Squeue(queue.name)
    queue.put(first_message)
    second_queue.put(second_message)
    queue.put(third_message)
    assert second_queue.get() == first_message
    assert queue.get() == second_message
    assert second_queue.get() == third_message


def test_get_returns_message_from_another_process(queue):
    message = "foo"
    data_queue = multiprocessing.Queue()
    get_process = multiprocessing.Process(
        target=put_data_to_queue, args=(queue.name, data_queue))
    get_process.start()
    data_queue.put(message)
    get_process.join()
    assert queue.get() == message


def test_put_writes_message_for_another_process(queue):
    message = "foo"
    data_queue = multiprocessing.Queue()
    get_process = multiprocessing.Process(
        target=get_data_from_queue, args=(queue.name, data_queue))
    get_process.start()
    queue.put(message)
    get_process.join()
    assert data_queue.get() == message


def test_empty_returns_true_for_new_queue(queue):
    assert queue.empty()


def test_empty_returns_false_if_there_is_data_in_the_queue(queue):
    message = "foo"
    queue.put(message)
    assert not queue.empty()


def test_empty_returns_true_for_each_queue_instance(queue):
    first_message = "foo"
    queue.put(first_message)
    second_queue = Squeue(queue.name)
    assert not second_queue.empty()
    second_message = "bar"
    second_queue.put(second_message)
    queue.get()
    queue.get()
    assert second_queue.empty()


def test_empty_not_changes_read_position(queue):
    first_message = "spam"
    second_message = "ham"
    third_message = "eggs"
    second_queue = Squeue(queue.name)
    queue.empty()
    queue.put(first_message)
    second_queue.empty()
    second_queue.put(second_message)
    queue.empty()
    queue.put(third_message)
    second_queue.empty()
    assert second_queue.get() == first_message
    queue.empty()
    assert queue.get() == second_message
    second_queue.empty()
    assert second_queue.get() == third_message
