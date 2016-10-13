"""SQueue implementation"""

from itertools import cycle
from itertools import islice
import os


class Squeue(object):
    """Named queue based on files"""

    _MESSAGE_SIZE_SIZE = 12
    """Size in bits of info about message length"""

    _MAX_MESSAGE_SIZE = 2 ** _MESSAGE_SIZE_SIZE
    """Max possible size of message to write in bytes"""

    _MESSAGE_READ_FLAG_FALSE = '0'
    """Available value for flag if message hasn't been read yet"""

    _MESSAGE_READ_FLAG_TRUE = '1'
    """Available value for flag if message has been read already"""

    _MESSAGE_READ_FLAG_SIZE = 1
    """Message read flag length in bits"""

    _CONTROL_BITS_RULE = '1'
    """Rule for control bits"""

    _CONTROL_BITS_MINIMAL_SIZE = 2
    """Minimal length of control bits section in bits"""

    _REQUIRED_METADATA_SIZE = (
        _MESSAGE_SIZE_SIZE +
        _MESSAGE_READ_FLAG_SIZE +
        _CONTROL_BITS_MINIMAL_SIZE)
    """Minimal required length of metadata"""

    _OPTIONAL_METADATA_SIZE = 8 - _REQUIRED_METADATA_SIZE % 8
    """Length of extra bits to fullfill byte size"""

    _CONTROL_BITS_SIZE = (
        _CONTROL_BITS_MINIMAL_SIZE + _OPTIONAL_METADATA_SIZE)
    """Length of control bits section"""

    _CONTROL_BITS = ''.join(
        islice(cycle(_CONTROL_BITS_RULE), None, _CONTROL_BITS_SIZE))
    """Control bits"""

    _METADATA_SIZE = (
        _REQUIRED_METADATA_SIZE + _OPTIONAL_METADATA_SIZE) // 8
    """Size in bytes of metadata"""

    name = None
    """Name of the queue"""

    _storage = None
    """Storage for messages"""

    _read_position = None
    """Read cursor position"""

    def _get_storage(self):
        """Returns storage for messages"""
        return os.open(self.name, os.O_RDWR | os.O_CREAT)

    def __init__(self, name):
        """Class constructor

        :param name: name of the queue
        :type name: str
        """
        self.name = name
        self._storage = self._get_storage()
        self._read_position = 0

    def put(self, message):
        """Puts message object to the queue

        :param message: object to put
        :type message: any
        """
        self._go_to_the_write_position()
        message = self._serialize_message(message)
        metadata = self._serialize_metadata(
            message_length=len(message),
            message_read=self._MESSAGE_READ_FLAG_FALSE)
        self._write(metadata)
        self._write(message)

    def get(self):
        """Returns message from the queue

        :rerturns: next object in the queue
        :rtype: any
        """
        self._go_to_the_read_position()
        while True:
            if self._is_end_of_file():
                return None
            if self._message_already_read():
                self._go_to_the_next_message()
            else:
                self._mark_message_as_read()
                return self._read_message()

    def empty(self):
        """Checks if there is not unprocessed messages in the queue

        :returns: if any messages in queue to process
        :rtype: bool
        """
        self._go_to_the_read_position()
        while True:
            if self._is_end_of_file():
                return True
            if self._message_already_read():
                self._go_to_the_next_message()
            else:
                return False

    def _read_message(self):
        """Read message from storage"""
        metadata = self._read(self._METADATA_SIZE)
        message_length, _ = self._deserialize_metadata(metadata)
        message = self._read(message_length)
        return self._deserialize_message(message)

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
        if not peek:
            self._read_position += len(data)
        else:
            self._go_to_the_read_position()
        return data

    def _write(self, value):
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
        metadata = self._read(self._METADATA_SIZE)
        if metadata:
            message_length, _ = self._deserialize_metadata(metadata)
            self._read_position += message_length
            self._go_to_the_read_position()

    def _is_end_of_file(self):
        """Checks if there is no messages in the storage

        :returns: is end of file succeed
        :rtype: bool
        """
        return not self._read(self._METADATA_SIZE, peek=True)

    def _message_already_read(self):
        """Checks if next message already proceed

        :returns: is message have been read already
        :rtype: bool
        """
        metadata = self._read(self._METADATA_SIZE, peek=True)
        _, message_read = self._deserialize_metadata(metadata)
        return message_read == self._MESSAGE_READ_FLAG_TRUE

    def _mark_message_as_read(self):
        """Mark current message as proceeded"""
        metadata = self._read(self._METADATA_SIZE, peek=True)
        message_length, _ = self._deserialize_metadata(metadata)
        new_metadata = self._serialize_metadata(
            message_length, self._MESSAGE_READ_FLAG_TRUE)
        self._write(new_metadata)
        self._go_to_the_read_position()

    def _serialize_metadata(self, message_length, message_read):
        """Creates metadata info based on message

        :param message: message to write
        :type message: bytes
        :returns: metadata info
        :rtype: bytes
        """
        message_length = self._int_to_binary(message_length)
        metadata = ''.join([message_length, message_read, self._CONTROL_BITS])
        return self._int_to_bytes(
            self._METADATA_SIZE, self._binary_to_int(metadata))

    def _deserialize_metadata(self, data):
        """Get metadata values from bytes

        :returns: metadata values
        :rtype: iterable
        """
        binary = self._int_to_binary(
            self._bytes_to_int(data), min_size=self._METADATA_SIZE * 8)
        # pylint: disable=unbalanced-tuple-unpacking
        message_length, message_read, control_bits = self._split_on_portions(
            binary, portions=[
                self._MESSAGE_SIZE_SIZE,
                self._MESSAGE_READ_FLAG_SIZE,
                self._CONTROL_BITS_SIZE])
        # pylint: enable=unbalanced-tuple-unpacking
        if control_bits != self._CONTROL_BITS:
            return None
        return self._binary_to_int(message_length), message_read

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

    @staticmethod
    def _split_on_portions(data, portions):
        """Splits data on portions

        :param data: data to split
        :type data: slicable
        :param portions: sizes of portions
        :type portions: list of int
        :returns: list of portions
        :rtype list
        """
        result = []
        for portion in portions:
            result.append(data[:portion])
            data = data[portion:]
        return result

    @staticmethod
    def _bytes_to_int(data):
        """Converts bytes to int value

        :param data: bytes data
        :type data: bytes
        :returns: converted value
        :rtype: int
        """
        return int.from_bytes(data, byteorder='big')

    @staticmethod
    def _int_to_bytes(length, value):
        """Converts int value to bytes

        :param value: value to convert
        :type value: int
        :returns: bytes data
        :rtype: bytes
        """
        return value.to_bytes(length, byteorder='big')

    @staticmethod
    def _int_to_binary(value, min_size=None):
        """Converts int value to binary string

        :param value: value to convert
        :type value: int
        :param min_size: result string length
        :type min_size: int or None
        :returns: binary form
        :rtype: str
        """
        value = "{0:b}".format(value)
        string_size = len(value)
        if min_size is not None and string_size < min_size:
            return "0" * (min_size - string_size) + value
        return value

    @staticmethod
    def _binary_to_int(binary):
        """Converts binary string to int value

        :param binary: binary form
        :type binary: str
        :returns: converted value
        :rtype: int
        """
        return int(binary, base=2)
