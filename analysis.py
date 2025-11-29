import pathlib
import polars

orders: polars.Dataframe = None

if pathlib.Path("orders.parquet").exists():
    orders = polars.read_parquet("orders.parquet")
else:
    for archive in pathlib.Path(".").glob("*.json"):
        print(f"Reading {archive}")
        if orders is None:
            orders = polars.read_json(archive).with_columns(polars.lit(archive.stem).alias("account"))
        else:
            orders = polars.concat([
                orders,
                polars.read_json(archive).with_columns(polars.lit(archive.stem).alias("account"))
            ])
    orders = orders.with_columns(
            polars.col("placedDate").str.to_datetime(time_unit="ms", time_zone="UTC").alias("placedDate")
    ).sort(["placedDate"])
    orders.write_parquet("orders.parquet")

buckets = orders.group_by_dynamic(
        index_column="placedDate",
        every="1h",
        period="1h",
        closed="left",
        group_by="account",
    ).agg(
        polars.len().alias("count")
    ).sort("placedDate")

buckets.write_csv("orders.csv")