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
posts: Final[str] = "posts"
cancels: Final[str] = "cancels"
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
    source: pathlib.Path,
) -> Optional[polars.schema.Schema]:
    if source.exists():
        return polars.Struct(polars.read_parquet_schema(source))
    return None

def save_schema(
    schema,
    destination: pathlib.Path,
) -> None:
    polars.DataFrame(schema=schema).write_parquet(destination)

def extract_catalogues(
    raw: pathlib.Path,
    normalised: pathlib.Path,
    schema: pathlib.Path,
    partition: str,
) -> None:
    _path = raw / catalogues / f"{partition}*"
    logger.info(f"Reading {catalogues} from {_path}")

    _catalogues = polars.scan_parquet(
        _path,
        glob=True,
        include_file_paths="path",
        cast_options=polars.ScanCastOptions(extra_struct_fields="ignore"),
    )

    _catalogues = _catalogues.filter(polars.col("response").struct.field("error").str.len_chars() == 0).select([
        polars.col("path"),
        polars.col("response").struct.field("timestamp").alias("timestamp"),
        polars.col("response").struct.field("body").alias("response"),
    ])

    _schema = load_schema(schema / f"{catalogues}.schema")
    if _schema is None:
        _schema = _catalogues.collect().get_column("response").str.json_decode(infer_schema_length=None).dtype
        save_schema(_schema, schema / f"{catalogues}.schema")
        logger.debug(f"Inferred schema")

    _catalogues = _catalogues.select([
        polars.col("path"),
        polars.col("timestamp").mul(1e9).cast(polars.Int64).cast(polars.Datetime("ns")).alias("timestamp"),
        polars.col("response").str.json_decode(_schema).alias("response"),
    ]).select([
        polars.col("path"),
        polars.col("timestamp"),
        polars.col("response").struct.field("result").alias("market"),
    ]).explode("market")

    logger.debug(f"Exploded markets")

    _markets = _catalogues.select([
        polars.col("path"),
        polars.col("timestamp"),
        polars.col("market").struct.field("marketId").alias("market_id"),
        polars.col("market").struct.field("marketName").alias("market_name"),
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
        polars.col("path"),
        polars.col("timestamp"),
        polars.col("market").struct.field("marketId").alias("market_id"),
        polars.col("market").struct.field("runners").alias("runner"),
    ]).explode("runner").select([
        polars.col("path"),
        polars.col("timestamp"),
        polars.col("market_id"),
        polars.col("runner").struct.field("runnerName").alias("runner_name"),
        polars.col("runner").struct.field("selectionId").alias("runner_id"),
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

    _path = normalised / "markets" / f"{catalogues}_{partition}.parquet"
    _markets.sink_parquet(_path, mkdir = True)
    logger.info(f"Wrote market {catalogues} to {_path}")

    _path = normalised / "runners" / f"{catalogues}_{partition}.parquet"
    _runners.sink_parquet(_path, mkdir = True)
    logger.info(f"Wrote runner {catalogues} to {_path}")

def extract_posts(
    raw: pathlib.Path,
    normalised: pathlib.Path,
    schema: pathlib.Path,
    partition: str,
) -> None:
    _path = raw / posts / f"{partition}*"
    logger.info(f"Reading {posts} from {_path}")

    _posts = polars.scan_parquet(
        _path,
        glob=True,
        include_file_paths="path",
        cast_options=polars.ScanCastOptions(extra_struct_fields="ignore"),
    )

    # There is a missing field, 'customerStrategyRef', which we would need to get from the request.
    # For the moment we'll ignore it, due to the fact that it is on the updates.
    _posts = _posts.filter(polars.col("response").struct.field("error").str.len_chars() == 0).select([
        polars.col("path"),
        polars.col("response").struct.field("timestamp").alias("timestamp"),
        polars.col("response").struct.field("body").alias("response"),
    ])

    _schema = load_schema(schema / f"{posts}.schema")
    if _schema is None:
        _schema = _posts.collect().get_column("response").str.json_decode(infer_schema_length=None).dtype
        save_schema(_schema, schema / f"{posts}.schema")
        logger.debug(f"Inferred schema")

    _posts = _posts.select([
        polars.col("path"),
        polars.col("timestamp").mul(1e9).cast(polars.Int64).cast(polars.Datetime("ns")).alias("timestamp"),
        polars.col("response").str.json_decode(_schema).alias("response"),
    ]).select([
        polars.col("path"),
        polars.col("timestamp"),
        polars.col("response").struct.field("id").alias("request_id"),
        polars.col("response").struct.field("result").alias("response"),
    ]).select([
        polars.col("path"),
        polars.col("timestamp"),
        polars.col("request_id"),
        polars.col("response").struct.field("customerRef").alias("customer_reference"),
        polars.col("response").struct.field("status").alias("status"),
        polars.col("response").struct.field("errorCode").alias("error_code"),
        polars.col("response").struct.field("marketId").alias("market_id"),
        polars.col("response").struct.field("instructionReports").alias("instruction_reports"),
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

    _path = normalised / "orders" / f"{posts}_{partition}.parquet"
    _posts.sink_parquet(_path, mkdir = True)
    logger.info(f"Wrote order {posts} to {_path}")

def extract_cancels(
    raw: pathlib.Path,
    normalised: pathlib.Path,
    schema: pathlib.Path,
    partition: str,
) -> None:
    _path = raw / cancels / f"{partition}*"
    logger.info(f"Reading {cancels} from {_path}")

    _cancels = polars.scan_parquet(
        _path,
        glob=True,
        include_file_paths="path",
        cast_options=polars.ScanCastOptions(extra_struct_fields="ignore"),
    )

    _cancels = _cancels.filter(polars.col("response").struct.field("error").str.len_chars() == 0).select([
        polars.col("path"),
        polars.col("response").struct.field("timestamp").alias("timestamp"),
        polars.col("response").struct.field("body").alias("response"),
    ])

    _schema = load_schema(schema / f"{cancels}.schema")
    if _schema is None:
        _schema = _cancels.collect().get_column("response").str.json_decode(infer_schema_length=None).dtype
        save_schema(_schema, schema / f"{cancels}.schema")
        logger.debug(f"Inferred schema")

    _cancels = _cancels.select([
        polars.col("path"),
        polars.col("timestamp").mul(1e9).cast(polars.Int64).cast(polars.Datetime("ns")).alias("timestamp"),
        polars.col("response").str.json_decode(_schema).alias("response"),
    ]).select([
            polars.col("path"),
            polars.col("timestamp"),
            polars.col("response").struct.field("id").alias("request_id"),
            polars.col("response").struct.field("result").alias("response"),
        ]).select([
            polars.col("path"),
            polars.col("timestamp"),
            polars.col("request_id"),
            polars.col("response").struct.field("customerRef").alias("customer_reference"),
            polars.col("response").struct.field("status").alias("status"),
            polars.col("response").struct.field("errorCode").alias("error_code"),
            polars.col("response").struct.field("marketId").alias("market_id"),
            polars.col("response").struct.field("instructionReports").alias("instruction_reports"),
        ]).explode("instruction_reports").with_columns([
            polars.col("instruction_reports").struct.field("status").alias("status"),
            polars.col("instruction_reports").struct.field("errorCode").alias("error_code"),
            polars.col("instruction_reports").struct.field("cancelledDate").alias("cancelled_date"),
            polars.col("instruction_reports").struct.field("sizeCancelled").alias("size_cancelled"),
            polars.col("instruction_reports").struct.field("instruction").struct.field("betId").alias("bet_id"),
        ]).drop("instruction_reports")

    logger.debug(f"Exploded instruction reports")

    _path = normalised / "orders" / f"{cancels}_{partition}.parquet"
    _cancels.sink_parquet(_path, mkdir = True)
    logger.info(f"Wrote order {cancels} to {_path}")

def extract_definitions(
    raw: pathlib.Path,
    normalised: pathlib.Path,
    schema: pathlib.Path,
    partition: str,
) -> None:
    _path = raw / definitions / f"{partition}*"
    logger.info(f"Reading {definitions} from {_path}")

    _definitions = polars.scan_parquet(
        _path,
        glob=True,
        include_file_paths="path",
        cast_options=polars.ScanCastOptions(extra_struct_fields="ignore"),
    )

    _definitions = _definitions.select([
        polars.col("path"),
        polars.col("timestamp").mul(1e9).cast(polars.Int64).cast(polars.Datetime("ns")).alias("timestamp"),
        polars.col("body").alias("message"),
    ])

    _schema = load_schema(schema / f"{definitions}.schema")
    if _schema is None:
        _schema = _definitions.collect().get_column("message").str.json_decode(infer_schema_length=None).dtype
        save_schema(_schema, schema / f"{definitions}.schema")
        logger.debug(f"Inferred schema")

    _definitions = _definitions.select([
        polars.col("path"),
        polars.col("timestamp"),
        polars.col("message").str.replace_all(r'"Infinity"|"NaN"', "null").str.json_decode(_schema).alias("message"),
    ]).select([
        polars.col("path"),
        polars.col("timestamp"),
        polars.col("message").struct.field("clk").alias("feed_id"),
        polars.col("message").struct.field("pt").alias("published_timestamp"),
        polars.col("message").struct.field("mc").alias("market_change"),
    ]).filter(polars.col("market_change").is_not_null())

    logger.debug(f"Dropped heartbeats")

    _definitions = _definitions.select([
        polars.col("path"),
        polars.col("timestamp"),
        polars.col("feed_id"),
        polars.col("published_timestamp"),
        polars.col("market_change").list.eval(polars.element().struct.field("id")).alias("market_id"),
        polars.col("market_change").list.eval(polars.element().struct.field("marketDefinition")).alias("market_definition"),
        polars.col("market_change").list.eval(polars.element().struct.field("tv")).alias("traded_value"),
    ]).explode("market_id", "market_definition", "traded_value")

    _markets = _definitions.select([
        polars.col("path"),
        polars.col("timestamp"),
        polars.col("feed_id"),
        polars.col("published_timestamp"),
        polars.col("market_id"),
        polars.col("market_definition").struct.field("bspMarket").alias("bsp_market"),
        polars.col("market_definition").struct.field("turnInPlayEnabled").alias("turn_in_play_enabled"),
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
        polars.col("path"),
        polars.col("timestamp"),
        polars.col("feed_id"),
        polars.col("published_timestamp"),
        polars.col("market_id"),
        polars.col("market_definition").struct.field("runners").alias("runner"),
    ]).explode("runner").select([
        polars.col("path"),
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

    _path = normalised / "markets" / f"{definitions}_{partition}.parquet"
    _markets.sink_parquet(_path, mkdir = True)
    logger.info(f"Wrote market {definitions} to {_path}")

    _path = normalised / "runners" / f"{definitions}_{partition}.parquet"
    _runners.sink_parquet(_path, mkdir = True)
    logger.info(f"Wrote runner {definitions} to {_path}")

def extract_updates(
    raw: pathlib.Path,
    normalised: pathlib.Path,
    schema: pathlib.Path,
    partition: str,
) -> None:
    _path = raw / updates / f"{partition}*"
    logger.info(f"Reading {updates} from {_path}")

    _updates = polars.scan_parquet(
        _path,
        glob=True,
        include_file_paths="path",
        cast_options=polars.ScanCastOptions(extra_struct_fields="ignore"),
    )

    _updates = _updates.select([
        polars.col("path"),
        polars.col("timestamp").mul(1e9).cast(polars.Int64).cast(polars.Datetime("ns")).alias("timestamp"),
        polars.col("body").alias("message"),
    ])

    _schema = load_schema(schema / "updates.schema")
    if _schema is None:
        _schema = _updates.collect().get_column("message").str.json_decode(infer_schema_length=None).dtype
        save_schema(_schema, schema / "updates.schema")
        logger.debug(f"Inferred schema")

    _updates = _updates.select([
        polars.col("path"),
        polars.col("timestamp"),
        polars.col("message").str.replace_all(r'"Infinity"|"NaN"', "null").str.json_decode(_schema).alias("message"),
    ]).select([
        polars.col("path"),
        polars.col("timestamp"),
        polars.col("message").struct.field("clk").alias("feed_id"),
        polars.col("message").struct.field("pt").alias("published_timestamp"),
        polars.col("message").struct.field("oc").alias("order_change"),
    ]).filter(polars.col("order_change").is_not_null())

    logger.debug(f"Dropped heartbeats")

    _updates = _updates.select([
        polars.col("path"),
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
        polars.col("orders").struct.field("id").alias("bet_id"),
        polars.col("orders").struct.field("p").alias("price"),
        polars.col("orders").struct.field("s").alias("size"),
        polars.col("orders").struct.field("bsp").alias("bsp_liability"),
        polars.col("orders").struct.field("side").alias("side"),
        polars.col("orders").struct.field("status").alias("status"),
        polars.col("orders").struct.field("pt").alias("persistence_type"),
        polars.col("orders").struct.field("ot").alias("order_type"),
        polars.col("orders").struct.field("pd").alias("placed_date"),
        polars.col("orders").struct.field("md").alias("matched_date"),
        polars.col("orders").struct.field("cd").alias("cancelled_date"),
        polars.col("orders").struct.field("ld").alias("lapsed_date"),
        polars.col("orders").struct.field("lsrc").alias("lapsed_status_reason_code"),
        polars.col("orders").struct.field("avp").alias("average_price_matched"),
        polars.col("orders").struct.field("sm").alias("size_matched"),
        polars.col("orders").struct.field("sr").alias("size_remaining"),
        polars.col("orders").struct.field("sl").alias("size_lapsed"),
        polars.col("orders").struct.field("sc").alias("size_cancelled"),
        polars.col("orders").struct.field("sv").alias("size_voided"),
        polars.col("orders").struct.field("rac").alias("regulator_authorization_code"),
        polars.col("orders").struct.field("rc").alias("regulator_code"),
        polars.col("orders").struct.field("rfo").alias("customer_order_reference"),
        polars.col("orders").struct.field("rfs").alias("customer_strategy_reference"),
    ])

    _path = normalised / "orders" / f"{updates}_{partition}.parquet"
    _updates.sink_parquet(_path, mkdir = True)
    logger.info(f"Wrote order {updates} to {_path}")


def main():
    """Parses command-line arguments and runs the main logic."""
    parser = argparse.ArgumentParser(
        description="Report on log files."
    )
    parser.add_argument(
        "-r",
        "--raw",
        required=True,
        type=str,
        help="The raw directory (contains the raw parquet files)."
    )
    parser.add_argument(
        "-n",
        "--normalised",
        required=True,
        type=str,
        help="The normalised directory to save the output."
    )
    parser.add_argument(
        "-s",
        "--schema",
        required=True,
        type=str,
        help="The schema directory."
    )
    parser.add_argument(
        "-p",
        "--partition",
        required=True,
        type=str,
        help="The partition used to import."
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

    _raw = pathlib.Path(_arguments.raw)
    _normalised = pathlib.Path(_arguments.normalised)
    _schema = pathlib.Path(_arguments.schema)
    _partition = _arguments.partition

    extract_catalogues(_raw, _normalised, _schema, _partition)
    extract_definitions(_raw, _normalised, _schema, _partition)
    extract_posts(_raw, _normalised, _schema, _partition)
    extract_cancels(_raw, _normalised, _schema, _partition)
    extract_updates(_raw, _normalised, _schema, _partition)

if __name__ == "__main__":
    try:
        main()
    except Exception as exception:
        logger.warning(f"An error occurred: {exception}")
        traceback.print_exc(file=sys.stdout)
