import argparse
import pathlib
import datetime
import logging
import traceback
import sys
from typing import Final, Callable
import json
import betfairlightweight

logger: Final[logging.Logger] = logging.getLogger(__name__)
count: Final[int] = 1000


def create_client(
        username: str,
        application: str,
        token: str,
):
    client = betfairlightweight.APIClient(
        username=username,
        password="placeholder",
        app_key=application,
    )

    client.set_session_token(token)
    logger.info(f"Client initialized with session token for user: {username}")
    return client


def paged_request(
        days: int,
        method,
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
                response = method(
                    date_range={
                        'from': _from.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        'to': _to.strftime("%Y-%m-%dT%H:%M:%SZ")
                    },
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


def list_cleared_orders(
        client: betfairlightweight.Betting,
        date_range,
        from_record,
        record_count,
):
    return client.list_cleared_orders(
        include_item_description=True,
        settled_date_range=date_range,
        from_record=from_record,
        record_count=record_count,
    )


def list_current_orders(
        client: betfairlightweight.Betting,
        date_range,
        from_record,
        record_count,
):
    return client.list_current_orders(
        include_item_description=True,
        date_range=date_range,
        from_record=from_record,
        record_count=record_count,
    )


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
        "-e",
        "--endpoint",
        required=True,
        type=str,
        choices=["cleared", "current"],
        help="The endpoint (cleared, current)."
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

    client = create_client(
        arguments.username,
        arguments.key,
        arguments.token,
    )

    if arguments.endpoint == "cleared":
        method = lambda date_range, from_record, record_count: list_cleared_orders(
            client.betting,
            date_range,
            from_record,
            record_count,
        )
    elif arguments.endpoint == "current":
        method = lambda date_range, from_record, record_count: list_current_orders(
            client.betting,
            date_range,
            from_record,
            record_count,
        )
    else:
        raise ValueError(f"Unknown endpoint {arguments.endpoint}")

    with open(pathlib.Path(arguments.archive), "a+t") as file:
        for response in paged_request(arguments.days, method):
            json.dump(response._data, file)


if __name__ == "__main__":
    try:
        main()
    except Exception as exception:
        logger.warning(f"An error occurred: {exception}")
        traceback.print_exc(file=sys.stdout)
