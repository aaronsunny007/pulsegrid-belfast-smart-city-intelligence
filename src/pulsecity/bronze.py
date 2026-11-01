"""Bronze: land raw envelopes in Delta, append-only, with lineage columns."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pulsecity.schemas import LANDING_ENVELOPE


def read_landing_stream(spark: SparkSession, landing_dir: str, schema_location: str) -> DataFrame:
    """Incrementally discover new landing files with Databricks Auto Loader."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("rescuedDataColumn", "_rescued_data")
        .schema(LANDING_ENVELOPE)
        .load(landing_dir)
    )


def read_landing_batch(spark: SparkSession, landing_dir: str) -> DataFrame:
    """Batch equivalent of read_landing_stream, for local runs where Auto Loader is absent."""
    return (
        spark.read.schema(LANDING_ENVELOPE)
        .option("recursiveFileLookup", "true")
        .json(landing_dir)
        .withColumn("_rescued_data", F.lit(None).cast("string"))
    )


def to_bronze(landing_df: DataFrame) -> DataFrame:
    """Type the poll timestamp and add lineage columns. The payload stays raw text."""
    return landing_df.select(
        "snapshot_id",
        "city",
        "source",
        "dataset",
        "endpoint",
        F.to_timestamp("polled_at").alias("polled_at"),
        "http_status",
        "elapsed_ms",
        "error",
        "payload",
        "_rescued_data",
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )
