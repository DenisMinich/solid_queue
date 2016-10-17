"""Squeue implementation"""

import os

from squeue import exceptions
from squeue.metadata import Metadata


class Squeue(object):
    """Named queue based on files"""

    _TOKEN_SIZE = 4
    """Size in bytes of data presents version of queue"""

    _START_POSITION = _TOKEN_SIZE
    """Position where messages start"""

    _DEFAULT_AUTOCLEAN = True
    """Default value for autoclean option"""

    _DEFAULT_CRITICAL_SIZE = 5 * 1024 * 1024  # 10Mb
    """Size of old messages to execute autoclean"""

    _read_position = 0
    """Position of next message to read"""

    def _get_storage(self):
        """Returns storage for messages"""
        return os.open(self.name, os.O_RDWR | os.O_CREAT)

    def __init__(
            self, name, autoclean=_DEFAULT_AUTOCLEAN,
            critical_size=_DEFAULT_CRITICAL_SIZE):
        """Class constructor

        :param name: name of the queue
        :type name: str
        """
        self.name = name
        self.autoclean = autoclean
        self.critical_size = critical_size
        self._storage = self._get_storage()
        if not self._check_token_is_empty():
            self._renew_token()
        self._token = self._get_token()
        self._read_position = self._go_to_start_position()

    def put(self, message):
        """Puts message object to the queue

        :param message: object to put
        :type message: any
        """
        self._go_to_end_position()
        metadata = Metadata(len(message), Metadata.MESSAGE_OLD_FLAG_FALSE)
        self._write(metadata.serialize())
        self._write(self._serialize_message(message))

    def get(self):
        """Returns message from the queue

        :rerturns: next message from the queue or None
        :rtype: str
        """
        self._validate_token()
        self._go_to_position(self._read_position)
        while not self._is_end_of_file():
            metadata = self._get_metadata()
            if metadata.message_old_flag == Metadata.MESSAGE_OLD_FLAG_FALSE:
                metadata.message_old_flag = Metadata.MESSAGE_OLD_FLAG_TRUE
                self._write(metadata.serialize())
                self._read_position += Metadata.METADATA_SIZE
                message = self._read(metadata.message_size)
                self._read_position = self._get_position()
                deserialized_message = self._deserialize_message(message)
                self._check_critical_size_reached()
                return deserialized_message
            self._read_position += metadata.full_message_size
            self._go_to_position(self._read_position)

    def empty(self):
        """Checks if there is not unprocessed messages in the queue

        :returns: if any messages in queue to process
        :rtype: bool
        """
        self._validate_token()
        self._go_to_position(self._read_position)
        while not self._is_end_of_file():
            metadata = self._get_metadata()
            if metadata.message_old_flag == Metadata.MESSAGE_OLD_FLAG_FALSE:
                return False
            self._read_position += metadata.full_message_size
            self._go_to_position(self._read_position)
        return True

    def clean(self):
        """Deletes old messages from the storage"""
        first_message_position = self._get_first_message_position()
        if first_message_position != self._START_POSITION:
            transfered_data_size = self._transfer_data(first_message_position)
            self._resize_storage(transfered_data_size + self._TOKEN_SIZE)
            self._renew_token()
            self._read_position = self._go_to_start_position()

    def _check_token_is_empty(self):
        """Check token exists

        :returns: is token exist
        :rtype: bool
        """
        self._go_to_position(0)
        return bool(self._read(self._TOKEN_SIZE, peek=True))

    def _renew_token(self):
        """Writes new token for queue"""
        token = self._generate_token(self._TOKEN_SIZE)
        self._go_to_position(0)
        self._write(token)

    @staticmethod
    def _generate_token(size):
        """Generates random bytes sequence

        :param size: size of token in bytes to generate
        :type size: int
        :returns: new token
        :rtype: bytes
        """
        return os.urandom(size)

    def _get_token(self):
        """Returns token of phisical storage

        :returns: storage's token
        :rtype: bytes
        """
        self._go_to_position(0)
        token = self._read(self._TOKEN_SIZE)
        if not token:
            raise exceptions.NoTokenFoundError("No token found")
        return token

    def _validate_token(self):
        """Checks if queue wasn't renewed"""
        if self._token != self._get_token():
            self._read_position = self._START_POSITION

    def _get_metadata(self):
        """Reads metadata for next message"""
        metadata_info = self._read(Metadata.METADATA_SIZE, peek=True)
        return Metadata.deserialize(metadata_info)

    def _read(self, length, peek=False):
        """Read portion of data from storage

        :param length: length in bytes of data
        :type length: int
        :param peek: if True - cursor position doesn't changes
        :type peek: bool
        :returns: data from file
        :rtype: bytes
        """
        data = os.read(self._storage, length)
        if peek:
            self._go_to_position(self._get_position() - len(data))
        return data

    def _write(self, value):
        """Writes bytes value into file

        :param value: value to write
        :type value: int
        """
        return os.write(self._storage, value)

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

    def _get_position(self):
        """Returns current position

        :returns: current cursor position
        :rtype: int
        """
        return os.lseek(self._storage, 0, os.SEEK_CUR)

    def _go_to_position(self, position):
        """Changes cursor position

        :param position: new position for cursor
        :type position: int
        """
        return os.lseek(self._storage, position, os.SEEK_SET)

    def _go_to_start_position(self, offset=0):
        """Moves cursot to the start

        :param offset: offset in bytes from the start
        :type offset: int
        """
        return os.lseek(self._storage, self._TOKEN_SIZE + offset, os.SEEK_SET)

    def _go_to_end_position(self, offset=0):
        """Moves cursot to the start

        :param offset: offset in bytes from the start
        :type offset: int
        """
        return os.lseek(self._storage, offset, os.SEEK_END)

    def _is_end_of_file(self):
        """Checks if there is no messages in the storage

        :returns: is end of file reached
        :rtype: bool
        """
        return not self._read(Metadata.METADATA_SIZE, peek=True)

    def _check_critical_size_reached(self):
        """Checks if sum size of old messages reached critical size"""
        if self.autoclean and self._read_position > self.critical_size:
            self.clean()

    def _get_first_message_position(self):
        """Returns position of first new message

        :returns: position of first new message
        :rtype: int
        """
        read_position = self._go_to_start_position()
        while not self._is_end_of_file():
            metadata = self._get_metadata()
            if metadata.message_old_flag == Metadata.MESSAGE_OLD_FLAG_FALSE:
                break
            read_position += metadata.full_message_size
            self._go_to_position(read_position)
        return read_position

    def _transfer_data(self, position, buffer_size=256):
        """Transfers valuable data from end of file to the start

        :param position: position where valuable part starts
        :type position: int
        :param buffer_size: size of buffer for data copying
        :type buffer_size: int
        :returns: result amount of data transfered
        :rtype: int
        """
        write_position = self._START_POSITION
        read_position = self._go_to_position(position)
        size_data_transfered = 0
        while True:
            self._go_to_position(read_position)
            data = self._read(buffer_size)
            if data:
                self._go_to_position(write_position)
                size_data = self._write(data)
                read_position += size_data
                write_position += size_data
                size_data_transfered += size_data
            else:
                break
        return size_data_transfered

    def _resize_storage(self, size):
        """Applies new size for storage

        :param size: new size for storage
        :type size: int
        """
        os.ftruncate(self._storage, size)
