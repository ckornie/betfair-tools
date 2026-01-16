import argparse
import pathlib
import logging
import sys
import re
import tempfile
import io
import os
import traceback
import configparser
from tarfile import TarInfo

import polars
import tarfile

from typing import Final, IO, Iterator, Dict, Tuple, BinaryIO, List

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

from b2sdk.v2 import B2Api, InMemoryAccountInfo, AuthInfoCache, DoNothingProgressListener


logger: Final[logging.Logger] = logging.getLogger(__name__)
key_size: Final[int] = 32
iv_size: Final[int] = 16

def configure_logging(
    verbosity: int,
) -> None:
    _library_level = logging.CRITICAL
    if verbosity >= 2:
        _library_level = logging.DEBUG
        logger.setLevel(level=logging.DEBUG)
    elif verbosity >= 1:
        logger.setLevel(level=logging.DEBUG)
    else:
        logger.setLevel(level=logging.INFO)

    logging.basicConfig(
        level=_library_level,
        format="%(asctime)s %(levelname)s (%(process)d): %(message)s",
    )

def read_configuration(
    configuration: pathlib.Path,
):
    _configuration = configparser.ConfigParser()
    _configuration.read(configuration)
    return _configuration

def backblaze(
    key_id: str,
    key: str,
) -> B2Api:
    logger.debug(f"Connecting with {key_id} and {key}")
    try:
        _account = InMemoryAccountInfo()
        _client = B2Api(_account, cache=AuthInfoCache(_account))
        _client.authorize_account(
            realm="production",
            application_key_id=key_id,
            application_key=key,
        )
        return _client
    except Exception as error:
        logger.error(f"Connection error: {error}")
        raise

def download_check(
    directory: pathlib.Path,
    download: str,
    pattern: str,
) -> bool:
    if "housekeeping" in download:
        logger.debug(f"Ignoring {download} due to housekeeping archive")
        return False

    if not re.match(pattern, download):
        logger.debug(f"Ignoring {download} due to pattern mismatch")
        return False

    _, _, _file = download.partition(r"/")
    _stem, _, _ = _file.partition(r".tar.zst")

    for _path in directory.rglob(f"{_stem}.parquet"):
        logger.info(f"Ignoring {download} due to existing file {_path}")
        return False
    return True

def download_all(
    client: B2Api,
    bucket: str,
    pattern: str,
    destination: pathlib.Path,
) -> Iterator[Tuple[str, IO]]:
    _bucket = client.get_bucket_by_name(bucket)
    for _file, _folder in _bucket.ls(latest_only=True, recursive=True):
        if download_check(destination, _file.file_name, pattern):
            logger.info(f"Downloading {_file.file_name}")
            _listener = DoNothingProgressListener()
            _download = _file.download(_listener)

            with tempfile.NamedTemporaryFile("w+b", dir=destination) as _handle:
                logger.debug(f"Saving to {_handle.name}")
                _download.save(_handle)
                _handle.flush()
                _handle.seek(0)

                (_, _, _name) = _file.file_name.partition("/")
                yield _name, _handle

def decrypt(
    name: str,
    data: IO,
    password: str,
    destination: pathlib.Path,
) -> BinaryIO:
    if data.read(8) != b'Salted__':
        raise ValueError("Input data does not start with magic string")

    _salt = data.read(8)
    _ciphertext = data.read()

    logger.debug(f"Length of ciphertext was {len(_ciphertext)}")

    _backend = default_backend()
    _key_iv = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=key_size + iv_size,
        salt=_salt,
        iterations=10_000,
        backend=default_backend(),
    ).derive(password.encode("utf-8"))

    _key = _key_iv[:key_size]
    _iv = _key_iv[key_size:]

    logger.debug(f"Key {_key.hex()} and initial vector {_iv.hex()}")

    _cipher = Cipher(
        algorithms.AES(_key),
        modes.CBC(_iv),
        backend=_backend,
    )

    _decryptor = _cipher.decryptor()
    _plaintext = _decryptor.update(_ciphertext) + _decryptor.finalize()
    _padding = _plaintext[-1]
    _plaintext = _plaintext[:-_padding]

    logger.debug(f"Length of plaintext was {len(_plaintext)} with padding of {_padding}")

    _path = destination / "archives" / name
    _handle = open(_path, "w+b")
    _handle.write(_plaintext)
    _handle.flush()
    _handle.seek(0)

    logger.debug(f"Wrote plaintext to {_handle.name}")
    return _handle


def network_logs(_members: List[TarInfo]) -> List[TarInfo]:
    return [_member for _member in _members if _member.isfile() and _member.size > 0 and "application-network.json" in _member.name]


def extract(
    name: str,
    tar: tarfile.TarFile,
    destination: pathlib.Path,
) -> bool:
    _buffers: Dict[str, IO] = {
        "catalogues": io.BytesIO(),
        "definitions": io.BytesIO(),
        "posts": io.BytesIO(),
        "cancels": io.BytesIO(),
        "updates": io.BytesIO(),
    }

    _logs: List[TarInfo] = network_logs(tar.getmembers())

    if len(_logs) != 1:
        logger.warning(f"Invalid file structure for {name}")
        return False

    for _log in _logs:
        logger.debug(f"Extracting {_log.name}")

        _filename, _, _ = _log.name.partition(r"/")

        with tar.extractfile(_log) as _file, io.TextIOWrapper(_file) as _log:
            for _line in _log:
                if "listMarketCatalogue" in _line:
                    _buffers["catalogues"].write(_line.encode('utf-8'))
                elif "marketDefinition" in _line:
                    _buffers["definitions"].write(_line.encode('utf-8'))
                elif "placeOrders" in _line:
                    _buffers["posts"].write(_line.encode('utf-8'))
                elif "cancelOrders" in _line:
                    _buffers["cancels"].write(_line.encode('utf-8'))
                elif r'\"op\":\"ocm\"' in _line:
                    _buffers["updates"].write(_line.encode('utf-8'))

        for _key, _value in _buffers.items():
            if _value.tell() > 0:
                _value.seek(0)

                _path = destination / _key / f"{_filename}.parquet"

                if _path.exists():
                    logger.warning(f"Ignored {_key} from {name}:{_filename} as {_path} exists")
                    return False
                else:
                    _path.parent.mkdir(parents=True, exist_ok=True)

                    try:
                        _df = polars.read_ndjson(_value).drop("telemetry", strict=False)
                        logger.debug(f"Schema of {_key} is {_df.schema}")

                        _df.write_parquet(_path)

                        logger.debug(f"Wrote {_key} from {name}:{_filename} to {_path}")
                    except Exception as _exception:
                        logger.warning(f"Did not write {_key} from {name}:{_filename} to {_path} due to {_exception}")
                        return False

            else:
                logger.info(f"No data for {_key} from {name}:{_filename}")

    return True


def process(
    name: str,
    archive: IO,
    password: str,
    destination: pathlib.Path,
    keep: bool,
) -> None:
    _zstd: IO = decrypt(name, archive, password, destination)
    with tarfile.open(fileobj=_zstd, mode='r:zst') as _tar:
        if extract(name, _tar, destination) and not keep:
            _zstd.close()
            os.remove(_zstd.name)
            logger.debug(f"Deleted {_zstd.name} after extraction")
        else:
            _zstd.close()
            logger.info(f"Saved {_zstd.name} for inspection")


def main():
    """Parses command-line arguments and runs the main logic."""
    parser = argparse.ArgumentParser(
        description="Parse log files."
    )
    parser.add_argument(
        "-f",
        "--file",
        required=False,
        type=str,
        help="Read directly from a file."
    )
    parser.add_argument(
        "-c",
        "--configuration",
        required=True,
        type=str,
        help="The configuration file."
    )
    parser.add_argument(
        "-p",
        "--pattern",
        required=False,
        type=str,
        default=".*.zst",
        help="The pattern used to match archives (e.g., '2025.*.zst')."
    )
    parser.add_argument(
        "-d",
        "--destination",
        required=True,
        type=str,
        help="The destination directory (also used for interim files)."
    )
    parser.add_argument(
        "-k",
        "--keep",
        required=False,
        action="store_true",
        help="Keep the downloaded archives."
    )
    parser.add_argument(
        "-v",
        "--verbose",
        required=False,
        action="count",
        help="Enable verbose logging."
    )

    _arguments = parser.parse_args()

    configure_logging(_arguments.verbose if _arguments.verbose else 0)

    _configuration = read_configuration(pathlib.Path(_arguments.configuration))
    _password = _configuration["archiving"]["archive_key"]
    _bucket = _configuration["archiving"]["backblaze_bucket"]
    _pattern = _arguments.pattern
    _keep = _arguments.keep

    _destination = pathlib.Path(_arguments.destination)
    _destination.mkdir(parents=True, exist_ok=True)

    if _arguments.file is not None:
        _file = pathlib.Path(_arguments.file)
        with _file.open(mode="rb") as _archive:
            process(_file.name, _archive, _password, _destination, _keep)
    else:
        _client = backblaze(
            _configuration["archiving"]["backblaze_key_id"],
            _configuration["archiving"]["backblaze_key"],
        )

        for (_name, _archive) in download_all(_client, _bucket, _pattern, _destination):
            process(_name, _archive, _password, _destination, _keep)

if __name__ == "__main__":
    try:
        main()
    except Exception as exception:
        logger.warning(f"An error occurred: {exception}")
        traceback.print_exc(file=sys.stdout)

