"""SQueue implementation"""

import os


class Squeue(object):

    _BUFER_SIZE = 256
    """Size of portion to read from file"""

    _name = None
    """Name of the queue"""

    _storage = None
    """Storage for messages"""

    def _get_storage(self):
        """Returns storage for messages"""
        return os.open(self._name, os.O_RDWR | os.O_CREAT)

    def __init__(self, name):
        """Class constructor

        :param name: name of the queue
        :type name: str
        """
        self._name = name
        self._storage = self._get_storage()

    def put(self, message):
        """Puts message object to the queue

        :param message: object to put
        :type message: any
        """
        os.lseek(self._storage, 0, os.SEEK_END)
        os.write(self._storage, str.encode(message))

    def get(self):
        """Returns message from the queue

        :rerturns: next object in the queue
        :rtype: any
        """
        os.lseek(self._storage, 0, os.SEEK_SET)
        bytes_read = os.read(self._storage, self._BUFER_SIZE)
        return bytes_read.decode()
