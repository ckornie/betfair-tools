import argparse
import pathlib
import datetime
import logging
import traceback
import sys
from typing import Final
import json
import betfairlightweight

logger: Final[logging.Logger] = logging.getLogger(__name__)
count: Final[int] = 1000


def cleared_orders(
        days: int,
        client: betfairlightweight.Betting,
):
    _yesterday = datetime.datetime.now() - datetime.timedelta(days=2)
    _to = datetime.datetime.combine(_yesterday.date(), datetime.time.min)

    while days >= 0:
        _ongoing: bool = True
        _record: int = 0
        _errors: int = 0

        _from = _to - datetime.timedelta(hours=24)
        logger.info(f"Requesting cleared orders from {_from:%Y-%m-%dT%H:%M} to {_to:%Y-%m-%dT%H:%M}")

        while _ongoing and _errors < 5:
            try:
                response = client.list_cleared_orders(
                    settled_date_range={
                        'from': _from.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        'to': _to.strftime("%Y-%m-%dT%H:%M:%SZ")
                    },
                    include_item_description=True,
                    from_record=_record,
                    record_count=count,
                )

                _ongoing = response.more_available
                _record = _record + len(response.orders)
                _errors = 0

                logger.debug(f"Recieved {_record} orders")

                yield response
            except Exception as exception:
                logger.warning(f"An error occurred: {exception}")
                _errors = _errors + 1

        logger.info(f"Recieved {_record} orders")

        _to = _from
        days = days - 1


def archive_cleared_orders(
        username: str,
        application: str,
        token: str,
        days: int,
        archive: pathlib.Path,
):
    trading = betfairlightweight.APIClient(
        username=username,
        password="placeholder",
        app_key=application,
    )

    trading.set_session_token(token)
    logger.info(f"Client initialized with session token for user: {username}")

    with open(archive, "a+t") as file:
        for response in cleared_orders(days, trading.betting):
            json.dump(response._data, file)


def main():
    """Parses command-line arguments and runs the main logic."""
    parser = argparse.ArgumentParser(
        description="Retrieve cleared Betfair orders using a pre-existing session token."
    )
    parser.add_argument(
        "-u",
        "--username",
        required=True,
        help="Your Betfair account username."
    )
    parser.add_argument(
        "-k",
        "--key",
        required=True,
        help="Your Betfair application key."
    )
    parser.add_argument(
        "-t",
        "--token",
        required=True,
        help="Your existing Betfair session token."
    )
    parser.add_argument(
        "-a",
        "--archive",
        required=True,
        help="The archive file."
    )
    parser.add_argument(
        "-d",
        "--days",
        required=True,
        type=int,
        help="The number of days to retreive."
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

    archive_cleared_orders(
        arguments.username,
        arguments.key,
        arguments.token,
        arguments.days,
        pathlib.Path(arguments.archive),
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exception:
        logger.warning(f"An error occurred: {exception}")
        traceback.print_exc(file=sys.stdout)
