import argparse
import pathlib
import logging
import sys
import traceback
import polars

from typing import Final, Optional, Any

logger: Final[logging.Logger] = logging.getLogger(__name__)
infer_count: Final[int] = 10_000

catalogues: Final[str] = "catalogues"
definitions: Final[str] = "definitions"
postings: Final[str] = "postings"
updates: Final[str] = "updates"

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

def load_schema(
    destination: pathlib.Path,
) -> Optional[polars.schema.Schema]:
    if destination.exists():
        return polars.Struct(polars.read_parquet_schema(destination))
    return None

def save_schema(
    schema,
    destination: pathlib.Path,
) -> None:
    polars.DataFrame(schema=schema).write_parquet(destination)

def extract_catalogues(
    source: pathlib.Path,
    destination: pathlib.Path,
) -> None:
    logger.info(f"Reading catalogues from {source}")

    _catalogues = polars.scan_delta(source / catalogues)

    _catalogues = _catalogues.filter(polars.col("response").struct.field("error").str.len_chars() == 0).select([
        polars.col("response").struct.field("timestamp").alias("timestamp"),
        polars.col("response").struct.field("body").alias("response"),
    ])

    _schema = load_schema(destination / f"{catalogues}.schema")
    if _schema is None:
        _schema = _catalogues.collect().get_column("response").str.json_decode(infer_schema_length=None).dtype
        save_schema(_schema, destination / f"{catalogues}.schema")
        logger.debug(f"Inferred schema")

    _catalogues = _catalogues.select([
        polars.col("timestamp").mul(1e9).cast(polars.Int64).cast(polars.Datetime("ns")).alias("timestamp"),
        polars.col("response").str.json_decode(_schema).alias("response"),
    ]).select([
        polars.col("timestamp"),
        polars.col("response").struct.field("result").alias("market"),
    ]).explode("market")

    logger.debug(f"Exploded markets")

    _markets = _catalogues.select([
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

    _runners = _catalogues.select([
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

    logger.debug(f"Exploded runners")

    _markets.sink_parquet(destination / catalogues / "markets.parquet", mkdir = True)
    _runners.sink_parquet(destination / catalogues / "runners.parquet", mkdir = True)
    logger.info(f"Wrote {catalogues} to {destination}")

def extract_postings(
    source: pathlib.Path,
    destination: pathlib.Path,
) -> None:
    logger.info(f"Reading postings from {source}")

    _postings = polars.scan_delta(source / postings)

    _postings = _postings.filter(polars.col("response").struct.field("error").str.len_chars() == 0).select([
        polars.col("response").struct.field("timestamp").alias("timestamp"),
        polars.col("response").struct.field("body").alias("response"),
    ])

    _schema = load_schema(destination / f"{postings}.schema")
    if _schema is None:
        _schema = _postings.collect().get_column("response").str.json_decode(infer_schema_length=None).dtype
        save_schema(_schema, destination / f"{postings}.schema")
        logger.debug(f"Inferred schema")

    _postings = _postings.select([
        polars.col("timestamp").mul(1e9).cast(polars.Int64).cast(polars.Datetime("ns")).alias("timestamp"),
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

    logger.debug(f"Exploded instruction reports")

    _postings.sink_parquet(destination / postings / f"{postings}.parquet", mkdir = True)
    logger.info(f"Wrote {postings} to {destination}")


def extract_definitions(
    source: pathlib.Path,
    destination: pathlib.Path,
) -> None:
    logger.info(f"Reading definitions from {source}")

    _definitions = polars.scan_delta(source / definitions)
    _definitions = _definitions.select([
        polars.col("timestamp").mul(1e9).cast(polars.Int64).cast(polars.Datetime("ns")).alias("timestamp"),
        polars.col("body").alias("message"),
    ])

    _schema = load_schema(destination / f"{definitions}.schema")
    if _schema is None:
        _schema = _definitions.collect().get_column("message").str.json_decode(infer_schema_length=None).dtype
        save_schema(_schema, destination / f"{definitions}.schema")
        logger.debug(f"Inferred schema")

    _definitions = _definitions.select([
        polars.col("timestamp"),
        polars.col("message").str.json_decode(_schema).alias("message"),
    ]).select([
        polars.col("timestamp"),
        polars.col("message").struct.field("clk").alias("feed_id"),
        polars.col("message").struct.field("pt").alias("published_timestamp"),
        polars.col("message").struct.field("mc").alias("market_change"),
    ]).filter(polars.col("market_change").is_not_null())

    logger.debug(f"Dropped heartbeats")

    _definitions = _definitions.select([
        polars.col("timestamp"),
        polars.col("feed_id"),
        polars.col("published_timestamp"),
        polars.col("market_change").list.eval(polars.element().struct.field("id")).alias("market_id"),
        polars.col("market_change").list.eval(polars.element().struct.field("marketDefinition")).alias("market_definition"),
        polars.col("market_change").list.eval(polars.element().struct.field("tv")).alias("traded_value"),
    ]).explode("market_id", "market_definition", "traded_value")

    _markets = _definitions.select([
        polars.col("timestamp"),
        polars.col("feed_id"),
        polars.col("published_timestamp"),
        polars.col("market_id"),
        polars.col("market_definition").struct.field("bspMarket").alias("bsp_market"),
        polars.col("market_definition").struct.field("turnInPlayEnabled").alias("name"),
        polars.col("market_definition").struct.field("persistenceEnabled").alias("persistence_enabled"),
        polars.col("market_definition").struct.field("marketBaseRate").alias("market_base_rate"),
        polars.col("market_definition").struct.field("eventId").alias("event_id"),
        polars.col("market_definition").struct.field("eventTypeId").alias("event_type_id"),
        polars.col("market_definition").struct.field("numberOfWinners").alias("number_of_winners"),
        polars.col("market_definition").struct.field("bettingType").alias("betting_type"),
        polars.col("market_definition").struct.field("marketType").alias("market_type"),
        polars.col("market_definition").struct.field("marketTime").alias("market_time"),
        polars.col("market_definition").struct.field("suspendTime").alias("suspend_time"),
        polars.col("market_definition").struct.field("bspReconciled").alias("bsp_reconciled"),
        polars.col("market_definition").struct.field("complete").alias("complete"),
        polars.col("market_definition").struct.field("inPlay").alias("in_play"),
        polars.col("market_definition").struct.field("crossMatching").alias("cross_matching"),
        polars.col("market_definition").struct.field("runnersVoidable").alias("runners_voidable"),
        polars.col("market_definition").struct.field("numberOfActiveRunners").alias("number_of_active_runners"),
        polars.col("market_definition").struct.field("betDelay").alias("bet_delay"),
        polars.col("market_definition").struct.field("status").alias("status"),
        polars.col("market_definition").struct.field("venue").alias("venue"),
        polars.col("market_definition").struct.field("countryCode").alias("country_code"),
        polars.col("market_definition").struct.field("discountAllowed").alias("discount_allowed"),
        polars.col("market_definition").struct.field("timezone").alias("timezone"),
        polars.col("market_definition").struct.field("openDate").alias("open_date"),
        polars.col("market_definition").struct.field("version").alias("version"),
        polars.col("market_definition").struct.field("raceType").alias("race_type"),
        polars.col("market_definition").struct.field("settledTime").alias("settled_time"),
        polars.col("traded_value"),
    ])

    logger.debug(f"Exploded markets")

    _runners = _definitions.select([
        polars.col("timestamp"),
        polars.col("feed_id"),
        polars.col("published_timestamp"),
        polars.col("market_id"),
        polars.col("market_definition").struct.field("runners").alias("runner"),
    ]).explode("runner").select([
        polars.col("timestamp"),
        polars.col("feed_id"),
        polars.col("published_timestamp"),
        polars.col("market_id"),
        polars.col("runner").struct.field("id").alias("runner_id"),
        polars.col("runner").struct.field("adjustmentFactor").alias("adjustment_factor"),
        polars.col("runner").struct.field("status").alias("status"),
        polars.col("runner").struct.field("sortPriority").alias("sort_priority"),
        polars.col("runner").struct.field("removalDate").alias("removal_date"),
        polars.col("runner").struct.field("bsp").alias("bsp"),
    ])

    logger.debug(f"Exploded runners")

    _markets.sink_parquet(destination / definitions / f"markets.parquet", mkdir = True)
    _runners.sink_parquet(destination / definitions / f"runners.parquet", mkdir = True)

    logger.info(f"Wrote {definitions} to {destination}")

def extract_updates(
    source: pathlib.Path,
    destination: pathlib.Path,
) -> None:
    logger.info(f"Reading updates from {source}")

    _updates = polars.scan_delta(source / updates)
    _updates = _updates.select([
        polars.col("timestamp").mul(1e9).cast(polars.Int64).cast(polars.Datetime("ns")).alias("timestamp"),
        polars.col("body").alias("message"),
    ])

    _schema = load_schema(destination / "updates.schema")
    if _schema is None:
        _schema = _updates.collect().get_column("message").str.json_decode(infer_schema_length=None).dtype
        save_schema(_schema, destination / "updates.schema")
        logger.debug(f"Inferred schema")

    _updates = _updates.select([
        polars.col("timestamp"),
        polars.col("message").str.json_decode(_schema).alias("message"),
    ]).select([
        polars.col("timestamp"),
        polars.col("message").struct.field("clk").alias("feed_id"),
        polars.col("message").struct.field("pt").alias("published_timestamp"),
        polars.col("message").struct.field("oc").alias("order_change"),
    ]).filter(polars.col("order_change").is_not_null())

    logger.debug(f"Dropped heartbeats")

    _updates = _updates.select([
        polars.col("timestamp"),
        polars.col("feed_id"),
        polars.col("published_timestamp"),
        polars.col("order_change").list.eval(polars.element().struct.field("id")).alias("market_id"),
        polars.col("order_change").list.eval(polars.element().struct.field("orc").list.eval(polars.element().struct.field("id"))).alias("runner_id"),
        polars.col("order_change").list.eval(polars.element().struct.field("orc").list.eval(polars.element().struct.field("uo"))).alias("orders"),
    ]).explode("market_id", "runner_id", "orders").explode("runner_id", "orders").explode("orders")

    logger.debug(f"Exploded orders")

    _updates = _updates.select([
        polars.exclude("orders"),
        polars.col("orders").struct.unnest(),
    ])

    _updates.sink_parquet(destination / updates / "updates.parquet", mkdir = True)
    logger.info(f"Wrote updates to {destination}")


def main():
    """Parses command-line arguments and runs the main logic."""
    parser = argparse.ArgumentParser(
        description="Report on log files."
    )
    parser.add_argument(
        "-s",
        "--source",
        required=True,
        type=str,
        help="The source directory (the raw parquet files)."
    )
    parser.add_argument(
        "-d",
        "--destination",
        required=True,
        type=str,
        help="The output directory."
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

    _source = pathlib.Path(_arguments.source)
    _destination = pathlib.Path(_arguments.destination)

    extract_catalogues(_source, _destination)
    extract_definitions(_source, _destination)
    extract_postings(_source, _destination)
    extract_updates(_source, _destination)

if __name__ == "__main__":
    try:
        main()
    except Exception as exception:
        logger.warning(f"An error occurred: {exception}")
        traceback.print_exc(file=sys.stdout)
