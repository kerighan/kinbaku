"""
Base exceptions and errors for Kinbaku
"""


__all__ = [
    "KinbakuException",
    "KinbakuError",
    "NodeNotFound",
    "EdgeNotFound"
]


class KinbakuException(Exception):
    """Base class for exceptions in Kinbaku."""


class KinbakuError(KinbakuException):
    """Exception for serious errors in Kinbaku."""


class NodeNotFound(KinbakuException):
    """Exception raised if requested node does not exist"""


class EdgeNotFound(KinbakuException):
    """Exception raised if requested edge does not exist."""


class KeyTooLong(KinbakuException):
    """Exception raised if node's key is too long."""
