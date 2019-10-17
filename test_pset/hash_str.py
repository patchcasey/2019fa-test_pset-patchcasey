from typing import AnyStr
import hashlib
from functools import wraps
import os
import pprint


def str_to_byte(func):
    """
    decorator adapted from https://forum.kodi.tv/showthread.php?tid=330975

    :param func: func taking string as arg
    :return: wrapped func
    """

    @wraps(func)
    def wrapped(*args, **kwargs):
        new_args = (x.encode() if isinstance(x, str) else x for x in args)
        new_kwargs = {
            k: v.encode() if isinstance(v, str) else v for k, v in kwargs.items()
        }
        return func(*new_args, **new_kwargs)

    return wrapped


def get_csci_salt(keyword="CSCI_SALT", convert_to_bytes="yes") -> bytes:
    """Returns the appropriate salt for CSCI E-29"""

    salt_hex = os.getenv(keyword)
    if convert_to_bytes == "yes":
        return bytes.fromhex(salt_hex)
    else:
        return salt_hex


@str_to_byte
def hash_str(some_val: AnyStr, salt: AnyStr = ""):
    """Converts strings to hash digest

    See: https://en.wikipedia.org/wiki/Salt_(cryptography)

    :param some_val: thing to hash

    :param salt: Add randomness to the hashing

    :rtype: bytes

    """
    m = hashlib.sha256()
    m.update(salt)
    m.update(some_val)
    # print(m.digest().hex()[:6])
    return m.digest()


def get_user_id(username: str) -> str:
    salt = get_csci_salt()
    return hash_str(username.lower(), salt=salt).hex()[:8]


if __name__ == "__main__":
    get_csci_salt()
