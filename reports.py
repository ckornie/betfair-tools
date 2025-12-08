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

def extract_catalogues(
    working: pathlib.Path,
) -> None:
    _catalogue = polars.read_delta(working / "catalogue")
    _catalogue = _catalogue.filter(polars.col("response").struct.field("error").str.len_chars() == 0).select([
        polars.col("response").struct.field("timestamp").alias("timestamp"),
        polars.col("response").struct.field("body").alias("response"),
    ])

    _schema = _catalogue.get_column("response").str.json_decode(infer_schema_length=None).dtype
    _catalogue = _catalogue.select([
        polars.col("timestamp"),
        polars.col("response").str.json_decode(_schema).alias("response"),
    ])

    _catalogue = _catalogue.select([
        polars.col("timestamp"),
        polars.col("response").struct.field("result").alias("market"),
    ]).explode("market")

    _markets = _catalogue.select([
        polars.col("timestamp"),
        polars.col("market").struct.field("marketId").alias("id"),
        polars.col("market").struct.field("marketName").alias("name"),
        polars.col("market").struct.field("marketStartTime").alias("start_time"),
        polars.col("market").struct.field("description").struct.field("persistenceEnabled").alias("persistence_enabled"),
        polars.col("market").struct.field("description").struct.field("bspMarket").alias("bsp_market"),
        polars.col("market").struct.field("description").struct.field("marketTime").alias("market_time"),
        polars.col("market").struct.field("description").struct.field("suspendTime").alias("suspend_time"),
        polars.col("market").struct.field("description").struct.field("bettingType").alias("betting_type"),
        polars.col("market").struct.field("description").struct.field("turnInPlayEnabled").alias("turn_in_play_enabled"),
        polars.col("market").struct.field("description").struct.field("marketType").alias("market_type"),
        polars.col("market").struct.field("description").struct.field("regulator").alias("regulator"),
        polars.col("market").struct.field("description").struct.field("marketBaseRate").alias("market_base_rate"),
        polars.col("market").struct.field("description").struct.field("discountAllowed").alias("discount_allowed"),
        polars.col("market").struct.field("description").struct.field("wallet").alias("wallet"),
        polars.col("market").struct.field("description").struct.field("rules").alias("rules"),
        polars.col("market").struct.field("description").struct.field("rulesHasDate").alias("rules_has_date"),
        polars.col("market").struct.field("description").struct.field("raceType").alias("race_type"),
        polars.col("market").struct.field("description").struct.field("clarifications").alias("clarifications"),
        polars.col("market").struct.field("totalMatched").alias("total_matched"),
        polars.col("market").struct.field("eventType").struct.field("id").alias("event_type_id"),
        polars.col("market").struct.field("eventType").struct.field("name").alias("event_type_name"),
        polars.col("market").struct.field("event").struct.field("id").alias("event_id"),
        polars.col("market").struct.field("event").struct.field("name").alias("event_name"),
        polars.col("market").struct.field("event").struct.field("countryCode").alias("event_country_code"),
        polars.col("market").struct.field("event").struct.field("timezone").alias("event_timezone"),
        polars.col("market").struct.field("event").struct.field("venue").alias("event_venue"),
        polars.col("market").struct.field("event").struct.field("openDate").alias("event_open_date"),
    ])

    _markets.write_csv("markets.csv")

    _runners = _catalogue.select([
        polars.col("timestamp"),
        polars.col("market").struct.field("marketId").alias("market_id"),
        polars.col("market").struct.field("runners").alias("runner"),
    ]).explode("runner").select([
        polars.col("timestamp"),
        polars.col("market_id"),
        polars.col("runner").struct.field("selectionId").alias("id"),
        polars.col("runner").struct.field("runnerName").alias("name"),
        polars.col("runner").struct.field("handicap").alias("handicap"),
        polars.col("runner").struct.field("sortPriority").alias("sort_priority"),
        polars.col("runner").struct.field("metadata").struct.field("SIRE_NAME").alias("sire_name"),
        polars.col("runner").struct.field("metadata").struct.field("CLOTH_NUMBER_ALPHA").alias("cloth_number_alpha"),
        polars.col("runner").struct.field("metadata").struct.field("OFFICIAL_RATING").alias("official_rating"),
        polars.col("runner").struct.field("metadata").struct.field("COLOURS_DESCRIPTION").alias("colours_description"),
        polars.col("runner").struct.field("metadata").struct.field("COLOURS_FILENAME").alias("colours_filename"),
        polars.col("runner").struct.field("metadata").struct.field("FORECASTPRICE_DENOMINATOR").alias("forecast_price_denominator"),
        polars.col("runner").struct.field("metadata").struct.field("DAMSIRE_NAME").alias("damsire_name"),
        polars.col("runner").struct.field("metadata").struct.field("WEIGHT_VALUE").alias("weight_value"),
        polars.col("runner").struct.field("metadata").struct.field("SEX_TYPE").alias("sex_type"),
        polars.col("runner").struct.field("metadata").struct.field("DAYS_SINCE_LAST_RUN").alias("days_since_last_run"),
        polars.col("runner").struct.field("metadata").struct.field("WEARING").alias("wearing"),
        polars.col("runner").struct.field("metadata").struct.field("OWNER_NAME").alias("owner_name"),
        polars.col("runner").struct.field("metadata").struct.field("DAM_YEAR_BORN").alias("dam_year_born"),
        polars.col("runner").struct.field("metadata").struct.field("SIRE_BRED").alias("sire_bred"),
        polars.col("runner").struct.field("metadata").struct.field("JOCKEY_NAME").alias("jockey_name"),
        polars.col("runner").struct.field("metadata").struct.field("DAM_BRED").alias("dam_bred"),
        polars.col("runner").struct.field("metadata").struct.field("ADJUSTED_RATING").alias("adjuster_rating"),
        polars.col("runner").struct.field("metadata").struct.field("runnerId").alias("runner_id"),
        polars.col("runner").struct.field("metadata").struct.field("CLOTH_NUMBER").alias("cloth_number"),
        polars.col("runner").struct.field("metadata").struct.field("SIRE_YEAR_BORN").alias("sire_year_born"),
        polars.col("runner").struct.field("metadata").struct.field("TRAINER_NAME").alias("trainer_name"),
        polars.col("runner").struct.field("metadata").struct.field("COLOUR_TYPE").alias("colour_type"),
        polars.col("runner").struct.field("metadata").struct.field("AGE").alias("age"),
        polars.col("runner").struct.field("metadata").struct.field("DAMSIRE_BRED").alias("damsire_bred"),
        polars.col("runner").struct.field("metadata").struct.field("JOCKEY_CLAIM").alias("jockey_claim"),
        polars.col("runner").struct.field("metadata").struct.field("FORM").alias("form"),
        polars.col("runner").struct.field("metadata").struct.field("FORECASTPRICE_NUMERATOR").alias("forecast_price_numerator"),
        polars.col("runner").struct.field("metadata").struct.field("BRED").alias("bred"),
        polars.col("runner").struct.field("metadata").struct.field("DAM_NAME").alias("dam_name"),
        polars.col("runner").struct.field("metadata").struct.field("DAMSIRE_YEAR_BORN").alias("damsire_year_born"),
        polars.col("runner").struct.field("metadata").struct.field("STALL_DRAW").alias("stall_draw"),
        polars.col("runner").struct.field("metadata").struct.field("WEIGHT_UNITS").alias("weight_units"),
    ])

    _runners.write_csv("runners.csv")

def extract_placements(
    working: pathlib.Path,
) -> None:
    _placement = polars.read_delta(working / "placement")

    _posted = _placement.filter(polars.col("response").struct.field("error").str.len_chars() == 0).select([
        polars.col("response").struct.field("timestamp").alias("timestamp"),
        polars.col("response").struct.field("body").alias("response"),
    ])

    _schema = _posted.get_column("response").str.json_decode(infer_schema_length=None).dtype

    _posted = _posted.select([
        polars.col("timestamp"),
        polars.col("response").str.json_decode(_schema).alias("response"),
    ]).select([
        polars.col("timestamp"),
        polars.col("response").struct.field("id").alias("id"),
        polars.col("response").struct.field("result").alias("posted"),
    ]).select([
        polars.col("timestamp"),
        polars.col("id"),
        polars.col("posted").struct.field("customerRef").alias("customer_reference"),
        polars.col("posted").struct.field("status").alias("status"),
        polars.col("posted").struct.field("errorCode").alias("error_code"),
        polars.col("posted").struct.field("marketId").alias("market_id"),
        polars.col("posted").struct.field("instructionReports").alias("instruction_reports"),
    ]).explode("instruction_reports").with_columns([
        polars.col("instruction_reports").struct.field("status").alias("status"),
        polars.col("instruction_reports").struct.field("errorCode").alias("error_code"),
        polars.col("instruction_reports").struct.field("betId").alias("bet_id"),
        polars.col("instruction_reports").struct.field("placedDate").alias("placed_date"),
        polars.col("instruction_reports").struct.field("averagePriceMatched").alias("average_price_matched"),
        polars.col("instruction_reports").struct.field("sizeMatched").alias("size_matched"),
        polars.col("instruction_reports").struct.field("orderStatus").alias("order_status"),
        polars.col("instruction_reports").struct.field("instruction").struct.field("selectionId").alias("selection_id"),
        polars.col("instruction_reports").struct.field("instruction").struct.field("customerOrderRef").alias("customer_order_reference"),
        polars.col("instruction_reports").struct.field("instruction").struct.field("orderType").alias("order_type"),
        polars.col("instruction_reports").struct.field("instruction").struct.field("side").alias("side"),
        polars.col("instruction_reports").struct.field("instruction").struct.field("limitOrder").struct.field("size").alias("size"),
        polars.col("instruction_reports").struct.field("instruction").struct.field("limitOrder").struct.field("price").alias("price"),
        polars.col("instruction_reports").struct.field("instruction").struct.field("limitOrder").struct.field("persistenceType").alias("persistence_type"),
    ]).drop("instruction_reports")

    _posted.write_csv("posted.csv")

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
    extract_catalogues(_working)
    extract_placements(_working)

if __name__ == "__main__":
    try:
        main()
    except Exception as exception:
        logger.warning(f"An error occurred: {exception}")
        traceback.print_exc(file=sys.stdout)
