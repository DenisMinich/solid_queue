"""SQueue implementation"""

import os


class Squeue(object):
    """Named queue based on files"""

    _MESSAGE_LENGTH_SIZE = 2
    """Size in bytes of info about message length"""

    _MAX_MESSAGE_LENGTH = 2 ** (_MESSAGE_LENGTH_SIZE * 8)
    """Max possible size of message to write"""

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
        message_in_bytes = self._serialize_message(message)
        message_length = len(message_in_bytes).to_bytes(
            self._MESSAGE_LENGTH_SIZE, byteorder='big')
        os.write(self._storage, message_length)
        os.write(self._storage, message_in_bytes)

    def get(self):
        """Returns message from the queue

        :rerturns: next object in the queue
        :rtype: any
        """
        os.lseek(self._storage, 0, os.SEEK_SET)
        message_length_data = os.read(self._storage, self._MESSAGE_LENGTH_SIZE)
        message_length = int.from_bytes(message_length_data, byteorder='big')
        message_data = os.read(self._storage, message_length)
        return self._deserialize_message(message_data)

    @staticmethod
    def _serialize_message(message):
        """Converts object to send into bytes array

        :param message: message to send
        :type message: any
        :returns: byte representation of the message
        :rtype: bytes
        """
        return str.encode(message)

    @staticmethod
    def _deserialize_message(data):
        """Converts bytes array into message object

        :param data: received data from storage
        :type data: bytes
        :returns: message was received
        :rtype: any
        """
        return data.decode()
