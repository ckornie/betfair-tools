import argparse
import pathlib
import logging
import sys
import traceback
import configparser
from typing import Final


logger: Final[logging.Logger] = logging.getLogger(__name__)

def read_configuration(
        configuration: pathlib.Path,
):
    _configuration = configparser.ConfigParser()
    _configuration.read(configuration)
    return _configuration


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
        "-v",
        "--verbose",
        required=False,
        type=bool,
        default=False,
        help="Enable verbose logging."
    )

    arguments = parser.parse_args()

    if arguments.verbose:
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    _configuration = read_configuration(pathlib.Path(arguments.configuration))
    logger.info(_configuration["archiving"]["archive_key"])
    logger.info(_configuration["archiving"]["backblaze_key_id"])
    logger.info(_configuration["archiving"]["backblaze_key"])

if __name__ == "__main__":
    try:
        main()
    except Exception as exception:
        logger.warning(f"An error occurred: {exception}")
        traceback.print_exc(file=sys.stdout)
