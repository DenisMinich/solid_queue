"""Lock implementation"""
from contextlib import contextmanager
from functools import wraps
import os
import time


class Lock(object):
    """Provides file based locks"""

    _DELAY = .01

    @staticmethod
    def acquire(lockfile, delay):
        """Acquires lockfile

        :param lockfile: path to lock file
        :type lockfile: str
        :param delay: time between lock tries
        :type delay: float
        """
        while True:
            try:
                return os.open(lockfile, os.O_CREAT | os.O_EXCL | os.O_RDWR)
            except OSError:
                time.sleep(delay)

    @staticmethod
    def release(lockfile, descriptor):
        """Releases lockfile

        :param lockfile: path to lock file
        :type lockfile: str
        :param descriptor: file descriptor of lockfile
        :type descriptor: int
        """
        os.close(descriptor)
        os.unlink(lockfile)

    @classmethod
    @contextmanager
    def lock_section(cls, lockfile, delay=_DELAY):
        """Locks file-based lock

        :param lockfile: path to lock file
        :type lockfile str
        """
        descriptor = cls.acquire(lockfile, delay)
        yield lockfile
        cls.release(lockfile, descriptor)

    @classmethod
    def wraps(cls, lockfile):
        """Decorator for function under lock"""
        def _decorator(func):
            @wraps(func)
            def _wrapper(*args, **kwargs):
                print('outside')
                with cls.lock_section(lockfile):
                    print('inside')
                    return func(*args, **kwargs)
            return _wrapper
        return _decorator
