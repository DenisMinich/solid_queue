"""Metadata implementation"""

from squeue.string_dispenser import StringDispenser


class Metadata(object):
    """Provides interface to work with message's metadata"""

    MESSAGE_SIZE_SIZE = 15
    """Size in bits of info about message length"""

    MAX_MESSAGE_SIZE = 2 ** MESSAGE_SIZE_SIZE
    """Max possible size of message to write in bytes"""

    MESSAGE_READ_FLAG_FALSE = '0'
    """Available value for flag if message hasn't been read yet"""

    MESSAGE_READ_FLAG_TRUE = '1'
    """Available value for flag if message has been read already"""

    MESSAGE_READ_FLAG_SIZE = 1
    """Message read flag length in bits"""

    METADATA_SIZE = (MESSAGE_SIZE_SIZE + MESSAGE_READ_FLAG_SIZE) // 8
    """Size in bytes of metadata"""

    def __init__(self, message_size, message_read_flag):
        self.message_size = message_size
        self.message_read_flag = message_read_flag

    @property
    def full_message_size(self):
        """Size of message with associated metadata"""
        return self.METADATA_SIZE + self.message_size

    @classmethod
    def deserialize(cls, data):
        """Generates Metadata instance based on provided data

        :param data: data to convert to Metadata
        :type data: bytes
        :returns: Metadata instance
        :rtype: Metadata
        """
        binary_metadata = cls._int_to_binary(
            cls._bytes_to_int(data), min_size=cls.METADATA_SIZE * 8)
        data_dispenser = StringDispenser(binary_metadata)
        message_read_flag = data_dispenser.get(cls.MESSAGE_READ_FLAG_SIZE)
        message_size_binary = data_dispenser.get(cls.MESSAGE_SIZE_SIZE)
        message_size = cls._binary_to_int(message_size_binary)
        return cls(message_size, message_read_flag)

    def serialize(self):
        """Serializes instance into bytes"""
        message_size_binary = self._int_to_binary(
            self.message_size, min_size=self.MESSAGE_SIZE_SIZE)
        metadata_bits = ''.join([self.message_read_flag, message_size_binary])
        return self._int_to_bytes(
            self.METADATA_SIZE, self._binary_to_int(metadata_bits))

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

    def __repr__(self):
        return "Metadata(message_size={!r}, message_read_flag={!r})".format(
            self.message_size, self.message_read_flag)
