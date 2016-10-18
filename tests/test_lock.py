# pylint: disable=invalid-name
# pylint: disable=missing-docstring
"""Integration tests on lock"""

from multiprocessing import Process
import os
import tempfile

from squeue.lock import Lock


lockfile = tempfile.NamedTemporaryFile().name


def sync_write_message_to_file_with_context(filename, message):
    descriptor = os.open(filename, os.O_WRONLY | os.O_CREAT)
    with Lock.lock_section(lockfile):
        os.lseek(descriptor, 0, os.SEEK_END)
        os.write(descriptor, message)


@Lock.wraps(lockfile)
def sync_write_message_to_file_with_decorator(filename, message):
    descriptor = os.open(filename, os.O_WRONLY | os.O_CREAT)
    os.lseek(descriptor, 0, os.SEEK_END)
    os.write(descriptor, message)


def fill_file(filename, processes_count, message_length, write_method):
    processes = []
    for i in range(processes_count):
        message = str(i) * message_length
        processes.append(
            Process(
                target=write_method, args=(filename, str.encode(message))))
    for process in processes:
        process.start()
    for process in processes:
        process.join()


def check_file(filename, processes_count, message_length):
    descriptor = os.open(filename, os.O_RDONLY | os.O_CREAT)
    for _ in range(processes_count):
        data = os.read(descriptor, message_length).decode()
        assert len(data) == message_length
        assert len(set(data)) == 1


def test_lock_dont_allow_async_write_to_file():
    filename = tempfile.NamedTemporaryFile().name
    processes_count = 10
    message_length = 100
    fill_file(
        filename, processes_count, message_length,
        write_method=sync_write_message_to_file_with_context)
    check_file(filename, processes_count, message_length)
    os.unlink(filename)


def test_lock_wraps_dont_allow_async_write_to_file():
    filename = tempfile.NamedTemporaryFile().name
    processes_count = 10
    message_length = 100
    fill_file(
        filename, processes_count, message_length,
        write_method=sync_write_message_to_file_with_decorator)
    check_file(filename, processes_count, message_length)
    os.unlink(filename)
