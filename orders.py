import argparse
import betfairlightweight
import datetime
import json

def get_cleared_orders(username, application, session_token):
    trading = betfairlightweight.APIClient(
        username=username,
        password="placeholder",
        app_key=application,
    )

    try:
        trading.set_session_token(session_token)
        print(f"✅ Client initialized with session token for user: {username}")

        _from = datetime.datetime(2025, 11, 20, 12, 0)
        _to = _from + datetime.timedelta(hours=2)

        # 4. Call the listClearedOrders method
        print("⏳ Requesting cleared orders...")
        cleared_orders = trading.betting.list_cleared_orders(
            bet_status="SETTLED",
            settled_date_range={
                'from': _from.strftime("%Y-%m-%dT%H:%M:%SZ"),
                'to': _to.strftime("%Y-%m-%dT%H:%M:%SZ")
            },
            include_item_description=True,
            record_count=100,
        )

        print(f"🎉 Found {len(cleared_orders.orders)} cleared orders.")

        for i, order in enumerate(cleared_orders.orders[:5]):
            print(f"Order {i+1}")
            print(f"Bet Id: {order.bet_id}")

    except Exception as exception:
        print(f"❌ An error occurred: {exception}")

    finally:
        # Note: No need to explicitly logout when using a session token,
        # as the token was provided externally.
        pass


def main():
    """Parses command-line arguments and runs the main logic."""
    parser = argparse.ArgumentParser(
        description="Retrieve cleared Betfair orders using a pre-existing session token."
    )
    parser.add_argument(
        '-u', '--username', required=True,
        help='Your Betfair account username.'
    )
    parser.add_argument(
        '-a', '--application', required=True,
        help='Your Betfair Application Key (App Key).'
    )
    parser.add_argument(
        '-t', '--token', required=True,
        help='Your existing Betfair session token (ssoid).'
    )

    args = parser.parse_args()

    # Run the main function with the parsed arguments
    get_cleared_orders(args.username, args.application, args.token)

if __name__ == "__main__":
    main()
