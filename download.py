import argparse
import pathlib
import logging
import sys
import re
import tempfile
import io
import traceback
import configparser
import polars
import tarfile

from typing import Final, IO, Iterator, Dict

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

def download_all(
    client: B2Api,
    bucket: str,
    pattern: str,
    working: pathlib.Path,
) -> Iterator[IO]:
    _bucket = client.get_bucket_by_name(bucket)
    for _file, _folder in _bucket.ls(latest_only=True, recursive=True):
        if re.match(pattern, _file.file_name):
            logger.info(f"Downloading {_file.file_name}")
            _listener = DoNothingProgressListener()
            _download = _file.download(_listener)

            with tempfile.NamedTemporaryFile("w+b", dir=working) as _handle:
                logger.debug(f"Saving to {_handle.name}")
                _download.save(_handle)
                _handle.flush()
                _handle.seek(0)
                yield _handle

def decrypt(
    data: IO,
    password: str,
    working: pathlib.Path,
) -> IO:
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

    _handle = tempfile.NamedTemporaryFile(mode="w+b", dir=working)
    _handle.write(_plaintext)
    _handle.flush()
    _handle.seek(0)

    logger.debug(f"Wrote plaintext to {_handle.name}")
    return _handle

def extract(
    tar: tarfile.TarFile,
    working: pathlib.Path,
) -> None:
    _buffers: Dict[str, IO] = {
        "catalogues": io.BytesIO(),
        "definitions": io.BytesIO(),
        "postings": io.BytesIO(),
        "updates": io.BytesIO(),
    }

    for _member in tar.getmembers():
        if _member.isfile() and "application-network.json" in _member.name:
            logger.info(f"Extracting {_member.name}")
            with tar.extractfile(_member) as _file, io.TextIOWrapper(_file) as _log:
                for _line in _log:
                    if "listMarketCatalogue" in _line:
                        _buffers["catalogues"].write(_line.encode('utf-8'))
                    elif "marketDefinition" in _line:
                        _buffers["definitions"].write(_line.encode('utf-8'))
                    elif "placeOrders" in _line:
                        _buffers["postings"].write(_line.encode('utf-8'))
                    elif "ocm" in _line:
                        _buffers["updates"].write(_line.encode('utf-8'))

    for _key, _value in _buffers.items():
        _value.seek(0)
        _path = working / _key

        logger.info(f"Writing {_key} to {_path}")
        _df = polars.read_ndjson(_value).drop("telemetry", strict=False)
        logger.debug(f"Schema of {_key} is {_df.schema}")
        _df.write_delta(_path, mode="append")

def main():
    """Parses command-line arguments and runs the main logic."""
    parser = argparse.ArgumentParser(
        description="Parse log files."
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
        "-w",
        "--working",
        required=True,
        type=str,
        help="The working directory (used for interim files)."
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
    _working = pathlib.Path(_arguments.working)

    _client = backblaze(
        _configuration["archiving"]["backblaze_key_id"],
        _configuration["archiving"]["backblaze_key"],
    )

    for _archive in download_all(_client, _bucket, _pattern, _working):
        with decrypt(_archive, _password, _working) as _zstd, tarfile.open(fileobj=_zstd, mode='r:zst') as _tar:
            extract(_tar, _working)


if __name__ == "__main__":
    try:
        main()
    except Exception as exception:
        logger.warning(f"An error occurred: {exception}")
        traceback.print_exc(file=sys.stdout)

