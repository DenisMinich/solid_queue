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

    _read_position = None
    """Read cursor position"""

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
        self._read_position = 0

    def put(self, message):
        """Puts message object to the queue

        :param message: object to put
        :type message: any
        """
        self._go_to_the_next_message()
        message = self._serialize_message(message)
        self._write_int(len(message), self._MESSAGE_LENGTH_SIZE)
        os.write(self._storage, message)

    def get(self):
        """Returns message from the queue

        :rerturns: next object in the queue
        :rtype: any
        """
        self._go_to_the_read_position()
        while True:
            message_length = self._read_int(self._MESSAGE_LENGTH_SIZE)
            if message_length is None:
                return None
            if message_length:
                message = self._read_bytes(message_length)
                self._read_position += (
                    message_length + self._MESSAGE_LENGTH_SIZE)
                return self._deserialize_message(message)
            self._go_to_the_next_message()

    def _read_int(self, length):
        """Read portion of data and converts it to int

        :param length: length in bytes of data
        :type length: int
        :returns: value from file
        :rtype: int
        """
        data = os.read(self._storage, length)
        if not data:
            return None
        return int.from_bytes(data, byteorder='big')

    def _write_int(self, value, length):
        """Writes int value into file

        :param value: value to write
        :type value: int
        :param length: length in bytes of data
        :type length: int
        """
        converted_data = value.to_bytes(length, byteorder='big')
        os.write(self._storage, converted_data)

    def _read_bytes(self, length):
        """Read portion of data and converts it to bytes

        :param length: length in bytes of data
        :type length: int
        :returns: data from file
        :rtype: bytes
        """
        data = os.read(self._storage, length)
        return data if data else None

    def _write_bytes(self, value):
        """Writes bytes value into file

        :param value: value to write
        :type value: int
        """
        os.write(self._storage, value)

    def _go_to_the_write_position(self):
        """Changes cursor position for writing"""
        os.lseek(self._storage, 0, os.SEEK_END)

    def _go_to_the_read_position(self):
        """Changes cursor position for reading"""
        os.lseek(self._storage, self._read_position, os.SEEK_SET)

    def _go_to_the_next_message(self):
        """Change cursor to the next message"""
        message_length = self._read_int(self._MESSAGE_LENGTH_SIZE)
        if message_length:
            self._read_position += (
                message_length + self._MESSAGE_LENGTH_SIZE)

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
