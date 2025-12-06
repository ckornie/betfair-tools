import argparse
import pathlib
import logging
import sys
import traceback
import polars

from typing import Final

logger: Final[logging.Logger] = logging.getLogger(__name__)

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

def report(
    working: pathlib.Path,
) -> None:
    _catalogue = polars.read_delta(working / "catalogue")

def main():
    """Parses command-line arguments and runs the main logic."""
    parser = argparse.ArgumentParser(
        description="Report on log files."
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

    _working = pathlib.Path(_arguments.working)
    report(_working)

if __name__ == "__main__":
    try:
        main()
    except Exception as exception:
        logger.warning(f"An error occurred: {exception}")
        traceback.print_exc(file=sys.stdout)
