"""StringDispenser implementation"""


# pylint: disable=too-few-public-methods
class StringDispenser(object):
    """Returns string by parts"""

    def __init__(self, data):
        self._data = data

    def get(self, size):
        """Returns part of string

        :param size: size of part
        :type size: int
        :returns: data part
        :rtype: str
        """
        part, self._data = self._data[:size], self._data[size:]
        return part

    def __repr__(self):
        return "StringDispenser(data={!r})".format(self._data)
