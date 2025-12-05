import argparse
import pathlib
import logging
import sys
import re
import traceback
import configparser
from b2sdk.v2 import B2Api, InMemoryAccountInfo, AuthInfoCache, DoNothingProgressListener
from typing import Final


logger: Final[logging.Logger] = logging.getLogger(__name__)
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

def extract(
    client: B2Api,
    bucket: str,
    pattern: str,
) -> None:
    _bucket = client.get_bucket_by_name(bucket)
    for _file, _folder in _bucket.ls(latest_only=True, recursive=True):
        if re.match(pattern, _file.file_name):
            logger.info(f"Downloading {_file.file_name}")
            _listener = DoNothingProgressListener()
            _download = client.download_file_by_id(_file.id_, _listener)  # only the headers
            logger.info(f"{_download}")
            # _download.save_to(local_file_path)  # this downloads the whole file

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
        "-v",
        "--verbose",
        action='store_true',
        help="Enable verbose logging."
    )

    arguments = parser.parse_args()

    if arguments.verbose:
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    _configuration = read_configuration(pathlib.Path(arguments.configuration))

    client = backblaze(
        _configuration["archiving"]["backblaze_key_id"],
        _configuration["archiving"]["backblaze_key"],
    )

    extract(
        client,
        _configuration["archiving"]["backblaze_bucket"],
        arguments.pattern,
    )

if __name__ == "__main__":
    try:
        main()
    except Exception as exception:
        logger.warning(f"An error occurred: {exception}")
        traceback.print_exc(file=sys.stdout)
