"""Exceptions for squeue module"""


class SqueueError(Exception):
    """Base exception for Squeue"""
    pass


class NoTokenFoundError(SqueueError):
    """Error occures if token section is empty"""
    pass


class ExcedeedMessageSizeError(SqueueError):
    """Error occures if max message size exceeded"""
    pass


