from __future__ import annotations  # type unions by | are only available in versions > 3.10

from bson import ObjectId
import hashlib
import json
import os
from collections.abc import Iterable
from contextlib import contextmanager
from datetime import datetime, timezone, date, time
from enum import Enum, unique
from itertools import islice
from pathlib import Path


def str_func_call(func, args, kwargs):
    args_list = [repr(arg) for arg in args] + [f"{key}={repr(val)}" for key, val in kwargs.items()]
    args_str = ", ".join(args_list)
    return f"{func.__name__}({args_str})"


def checksum(fname: Path | str):
    m = hashlib.md5()
    with open(str(fname), "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            m.update(chunk)
    return m.hexdigest()


#
# def checksum_py311(fname):
#     with open(fname, 'rb') as f:
#         digest = hashlib.file_digest(f, 'md5')
#         return digest.hexdigest()


def parse_number(x, default=None, func=int):
    if x is None:
        return x
    try:
        return func(x)
    except ValueError:
        return default


def convert_size_to_bytes(size_str: str) -> int:
    num, unit = size_str[:-1], size_str[-1]
    if unit == "K":
        return int(float(num) * 1024)
    elif unit == "M":
        return int(float(num) * 1024 ** 2)
    elif unit == "G":
        return int(float(num) * 1024 ** 3)
    elif unit == "T":
        return int(float(num) * 1024 ** 4)
    else:
        return parse_number(size_str, default=size_str)


def merge(a: dict, b: dict) -> dict:
    """
    "merges b into a" - overwrites values of a with that of b for conflicting keys

    a = {
        1: {"a":"A"},
        2: {"b":"B"},
        3: [1,2,3],
        4: {'a': {'b': 2}}
    }

    b = {
        2: {"c":"C"},
        3: {"d":"D"},
        4: {'c': {'b': 3}, 'a': [1,2,{'b':2}]}
    }

    merge(a,b)
    {
        1: {'a': 'A'},
        2: {'b': 'B', 'c': 'C'},
        3: {'d': 'D'},
        4: {'a': [1, 2, {'b': 2}], 'c': {'b': 3}}
    }
    """

    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge(a[key], b[key])
            else:
                a[key] = b[key]
        else:
            a[key] = b[key]
    return a


def is_readable(f: Path):
    if f.is_file() and os.access(str(f), os.R_OK):
        return True
    if f.is_dir() and os.access(str(f), os.R_OK | os.X_OK):
        return True
    return False


def batched(iterable: Iterable, n: int) -> list:
    """Batch data into lists of length n. The last batch may be shorter."""
    # batched('ABCDEFG', 3) --> ABC DEF G
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            return
        yield batch


@contextmanager
def empty_context_manager():
    try:
        yield
    finally:
        pass


@unique
class FileType(str, Enum):
    FILE = 'file'
    DIRECTORY = 'directory'
    SYMBOLIC_LINK = 'symbolic link'
    OTHER = 'other'


def filetype(p: Path) -> FileType:
    if p.is_symlink():
        return FileType.SYMBOLIC_LINK
    if p.is_file():
        return FileType.FILE
    if p.is_dir():
        return FileType.DIRECTORY
    return FileType.OTHER


def current_time_iso8601() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class JSONEncoder(json.JSONEncoder):
    """
    This class instantiates a custom JSON encoder that extends the utilities of json.JSONEncoder.

    This custom encoder provides flexible handling for datetime-like objects and MongoDB ObjectId-like objects.
    It allows for custom type specifications and falls back to the default JSON encoding for unhandled types.

    Attributes:
        datetime_types (tuple): A tuple of types to be treated as datetime-like objects.
        object_id_types (tuple): A tuple of types to be treated as ObjectId-like objects.

    Usage:
        encoder = JSONEncoder(datetime_types=(datetime, date, CustomDate),
                              object_id_types=(ObjectId, CustomObjectId))
        json_string = json.dumps(data, cls=encoder)
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the JSONEncoder with custom type handling.

        Args:
            *args: Variable length argument list passed to json.JSONEncoder.
            **kwargs: Arbitrary keyword arguments passed to json.JSONEncoder.
            datetime_types (tuple, optional): Types to be treated as datetime-like objects.
                                              Defaults to (datetime, date, time).
            object_id_types (tuple, optional): Types to be treated as ObjectId-like objects.
                                               Defaults to (ObjectId,).
        """
        self.datetime_types = kwargs.pop('datetime_types', (datetime, date, time))
        self.object_id_types = kwargs.pop('object_id_types', (ObjectId,))
        super().__init__(*args, **kwargs)

    def default(self, obj):
        """
        Implement custom serialization for datetime-like and ObjectId-like objects.

        This method is called for objects that aren't natively serializable by json.
        It handles the custom types specified in datetime_types and object_id_types,
        converting them to JSON-serializable formats.

        Args:
            obj: The object to serialize.

        Returns:
            str: A JSON-serializable representation of the object.

        Note:
            - Datetime-like objects are converted to ISO 8601 format strings.
            - ObjectId-like objects are converted to their string representation.
            - For all other types, it falls back to the default json.JSONEncoder behavior.
        """
        if isinstance(obj, self.datetime_types):
            return obj.isoformat()
        if isinstance(obj, self.object_id_types):
            return str(obj)
        # if isinstance(obj, bytes):
        #     return obj.decode('utf-8')
        return super().default(obj)
