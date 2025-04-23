"""Helper methods for use with Refrapt."""

from re import sub
from os.path import isfile
from gzip import open as gzip_open
from lzma import open as lzma_open
from bz2 import open as bz2_open
from shutil import copyfileobj
from logging import Logger

def convert_to_bytes(size_str: str) -> int:
    """Convert size suffix to bytes"""

    size_str = size_str.strip().lower()

    suffixes = {
        'b': 1,
        'k': 1024,
        'm': 1024 * 1024,
        'g': 1024 * 1024 * 1024,
        't': 1024 * 1024 * 1024 * 1024,
    }

    for suffix, factor in suffixes.items():
        if size_str.endswith(suffix):
            size_value = float(size_str[:-len(suffix)])
            return int(size_value * factor)

    return int(size_str)

def SanitiseUri(uri: str) -> str:
    """Sanitise a Uri so it is suitable for filesystem use."""
    uri = sub(r"^(\w+)://", "", uri)
    uri = sub(r":\d+", "", uri) # Port information

    return uri

def UnzipFile(file: str):
    """
        Finds the first file matching a supported compression format and unzips it.

        Section 1.1.2 of DebianRepository Format document states:
        - "an index may be compressed in one or multiple of the following formats:
            - No compression (no extension)
            - XZ (.xz extension)
            - Gzip (.gz extension, usually for Contents files, and diffs)
            - Bzip2 (.bz2 extension, usually for Translations)
            - LZMA (.lzma extension)

            Clients must support xz compression, and must support gzip and bzip2 if they want to 
            use the files that are listed as usual use cases of these formats. Support for all 
            three formats is highly recommended, as gzip and bzip2 are historically more widespread.

            Servers should offer only xz compressed files, except for the special cases listed above. 
            Some historical clients may only understand gzip compression, if these need to be 
            supported, gzip-compressed files may be offered as well."
        - https://wiki.debian.org/DebianRepository/Format#Compression_of_indices

        Therefore, prefer .xz files.
    """

    if isfile(f"{file}.xz"):
        with lzma_open(f"{file}.xz", "rb") as f:
            with open(file, "wb") as out:
                copyfileobj(f, out)
    elif isfile(f"{file}.gz"):
        with gzip_open(f"{file}.gz", "rb") as f:
            with open(file, "wb") as out:
                copyfileobj(f, out)
    elif isfile(f"{file}.bz2"):
        with bz2_open(f"{file}.bz2", "rb") as f:
            with open(file, "wb") as out:
                copyfileobj(f, out)
    else:
        return TypeError(f"File '{file}' has an unsupported compression format")
