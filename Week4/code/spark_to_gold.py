import argparse
import logging
import os
import re
from pathlib import Path
from typing import Dict, Iterable, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

LOGGER = logging.getLogger("spark_to_gold")
ENV_VAR_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read Silver Parquet from S3 and build Gold curated tables."
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config file (default: config.yaml).",
    )
    return parser.parse_args()


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def resolve_placeholders(config_text: str) -> str:
    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        value = os.getenv(key)
        return value if value is not None else match.group(0)

    return ENV_VAR_PATTERN.sub(replace, config_text)


def load_dotenv_if_present(dotenv_path: str = ".env") -> None:
    path = Path(dotenv_path)
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        parts = stripped.split("=", 1)
        if len(parts) != 2:
            continue
        key = parts[0].strip()
        value = parts[1].strip()
        if key and key not in os.environ:
            os.environ[key] = value


def parse_simple_yaml(config_text: str) -> Dict:
    config: Dict = {}
    current_parent = config
    current_parent_key = None

    for raw_line in config_text.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        indent = len(line) - len(line.lstrip(" "))
        if ":" not in stripped:
            raise ValueError(f"Invalid config line (missing ':'): {raw_line}")

        key, value = stripped.split(":", 1)
        key = key.strip()
        value = value.strip()

        if indent == 0:
            if not value:
                config[key] = {}
                current_parent = config[key]
                current_parent_key = key
            else:
                config[key] = value
                current_parent = config
                current_parent_key = None
            continue

        if indent == 2:
            if current_parent_key is None or not isinstance(current_parent, dict):
                raise ValueError(f"Invalid nested config structure near line: {raw_line}")
            current_parent[key] = value
            continue

        raise ValueError(
            "Unsupported config indentation. Only top-level and one nested level are supported."
        )

    return config


def load_config(config_path: str) -> Dict:
    load_dotenv_if_present()

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    config_text = path.read_text(encoding="utf-8")
    resolved_text = resolve_placeholders(config_text)
    config = parse_simple_yaml(resolved_text)

    if not isinstance(config, dict):
        raise ValueError("Config must be a YAML dictionary.")

    return config


def get_nested(config: Dict, keys: Iterable[str]):
    current = config
    key_path = ".".join(keys)

    for key in keys:
        if not isinstance(current, dict) or key not in current:
            raise ValueError(f"Missing required config key: {key_path}")
        current = current[key]

    return current


def get_required_string(config: Dict, keys: Iterable[str]) -> str:
    value = get_nested(config, keys)
    text = str(value).strip()

    if not text:
        raise ValueError(f"Config value is empty: {'.'.join(keys)}")

    if "${" in text:
        raise ValueError(
            f"Unresolved placeholder for {'.'.join(keys)}: {text}. "
            "Set the required environment variable(s) before running."
        )

    return text


def parse_bool(value, key_name: str) -> bool:
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y"}:
            return True
        if lowered in {"0", "false", "no", "n"}:
            return False

    raise ValueError(f"Invalid boolean value for {key_name}: {value}")


def parse_positive_int(value, key_name: str) -> int:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid integer value for {key_name}: {value}") from exc

    if parsed <= 0:
        raise ValueError(f"Value for {key_name} must be > 0, got: {parsed}")

    return parsed


def choose_column(columns: Iterable[str], candidates: Iterable[str], label: str) -> str:
    column_set = set(columns)

    for candidate in candidates:
        if candidate in column_set:
            LOGGER.info("Using %s column: %s", label, candidate)
            return candidate

    for candidate in candidates:
        for column in columns:
            if candidate in column:
                LOGGER.info("Using %s fallback column: %s", label, column)
                return column

    raise ValueError(
        f"Could not find a suitable {label} column. Candidates: {list(candidates)}. "
        f"Available columns: {list(columns)}"
    )


def normalized_text_col(column_name: str):
    text_col = F.trim(F.col(column_name).cast("string"))
    return F.when(text_col == "", F.lit(None)).otherwise(text_col)


def numeric_value_col(column_name: str):
    cleaned = F.regexp_replace(F.trim(F.col(column_name).cast("string")), ",", "")
    return cleaned.cast("double")


def prepare_base_dataframe(silver_df: DataFrame) -> DataFrame:
    required_partition_columns = {"year", "month"}
    available = set(silver_df.columns)

    missing_partition_cols = required_partition_columns - available
    if missing_partition_cols:
        raise ValueError(f"Silver data is missing partition columns: {sorted(missing_partition_cols)}")

    country_col = choose_column(
        silver_df.columns,
        ["country", "area", "area_name", "region", "economy", "country_name"],
        "country",
    )
    flow_col = choose_column(
        silver_df.columns,
        ["flow", "flow_name", "flow_breakdown", "transaction", "activity"],
        "flow",
    )
    product_col = choose_column(
        silver_df.columns,
        ["product", "product_name", "commodity", "energy_product"],
        "product",
    )
    value_col = choose_column(
        silver_df.columns,
        ["value", "obs_value", "quantity", "volume", "amount"],
        "value",
    )

    base_df = silver_df.select(
        F.col("year").cast("int").alias("year"),
        F.col("month").cast("int").alias("month"),
        normalized_text_col(country_col).alias("country"),
        F.upper(normalized_text_col(flow_col)).alias("flow"),
        F.upper(normalized_text_col(product_col)).alias("product"),
        numeric_value_col(value_col).alias("value"),
    )

    base_df = base_df.filter(
        F.col("year").isNotNull()
        & F.col("month").isNotNull()
        & (F.col("month") >= 1)
        & (F.col("month") <= 12)
        & F.col("value").isNotNull()
    )

    return base_df


def assert_non_empty(df: DataFrame, label: str, base_df: DataFrame = None) -> None:
    if df.limit(1).count() > 0:
        return

    if base_df is None:
        raise ValueError(f"No rows found for {label}.")

    sample_flows = [
        row["flow"]
        for row in base_df.select("flow").where(F.col("flow").isNotNull()).distinct().orderBy("flow").limit(20).collect()
    ]
    raise ValueError(
        f"No rows matched {label}. Sample available flow values: {sample_flows}"
    )


def write_gold_table(
    df: DataFrame,
    gold_uri: str,
    table_name: str,
    target_files_per_partition: int,
) -> None:
    output_path = gold_uri.rstrip("/") + f"/{table_name}"
    row_count = df.count()

    if row_count == 0:
        raise ValueError(f"No rows to write for {table_name}.")

    distinct_partition_count = df.select("year", "month").distinct().count()
    target_write_partitions = max(1, distinct_partition_count * target_files_per_partition)

    LOGGER.info("Writing %s rows to %s", row_count, output_path)
    LOGGER.info("Distinct partitions for %s: %s", table_name, distinct_partition_count)
    LOGGER.info("Target write partitions for %s: %s", table_name, target_write_partitions)

    (
        df.repartition(target_write_partitions, "year", "month")
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(output_path)
    )


def main() -> None:
    configure_logging()
    args = parse_args()

    config = load_config(args.config)
    mode = str(config.get("mode", "")).strip().lower()
    if mode and mode != "aws":
        LOGGER.warning("Config mode is '%s'. Expected 'aws' for this pipeline.", mode)

    silver_uri = get_required_string(config, ["silver_uri"])
    gold_uri = get_required_string(config, ["gold_uri"])

    adaptive_enabled = parse_bool(get_nested(config, ["spark", "adaptive_enabled"]), "spark.adaptive_enabled")
    shuffle_partitions = parse_positive_int(
        get_nested(config, ["spark", "shuffle_partitions"]),
        "spark.shuffle_partitions",
    )
    target_files_per_partition = parse_positive_int(
        get_nested(config, ["spark", "target_files_per_partition"]),
        "spark.target_files_per_partition",
    )

    spark = (
        SparkSession.builder.appName("jodi_oil_spark_to_gold")
        .config("spark.sql.adaptive.enabled", str(adaptive_enabled).lower())
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )

    try:
        LOGGER.info("Reading Silver Parquet from %s", silver_uri)
        silver_df = spark.read.parquet(silver_uri.rstrip("/"))
        if not silver_df.columns:
            raise ValueError(f"No columns detected in silver input: {silver_uri}")

        base_df = prepare_base_dataframe(silver_df).cache()
        assert_non_empty(base_df, "base dataframe")

        production_df = base_df.filter(F.col("flow").rlike("PROD|PRODUCTION"))
        assert_non_empty(production_df, "production rows", base_df)

        imports_df = (
            base_df.filter(F.col("flow").rlike("IMPORT|IMP"))
            .groupBy("country", "year", "month")
            .agg(F.sum("value").alias("imports_value"))
        )

        exports_df = (
            base_df.filter(F.col("flow").rlike("EXPORT|EXP"))
            .groupBy("country", "year", "month")
            .agg(F.sum("value").alias("exports_value"))
        )

        if imports_df.limit(1).count() == 0 and exports_df.limit(1).count() == 0:
            raise ValueError("No import/export rows found; cannot build trade balance table.")

        gold_monthly_global_production = (
            production_df.groupBy("year", "month")
            .agg(F.sum("value").alias("total_production"))
            .withColumn("metric_name", F.lit("global_production"))
            .select("metric_name", "total_production", "year", "month")
        )

        gold_country_production_trend = (
            production_df.filter(F.col("country").isNotNull())
            .groupBy("country", "year", "month")
            .agg(F.sum("value").alias("production_value"))
            .select("country", "production_value", "year", "month")
        )

        rank_window = Window.partitionBy("year", "month").orderBy(
            F.col("production_value").desc(),
            F.col("country").asc(),
        )

        gold_top_producers_by_month = (
            gold_country_production_trend.withColumn("producer_rank", F.row_number().over(rank_window))
            .filter(F.col("producer_rank") <= 10)
            .select("country", "production_value", "producer_rank", "year", "month")
        )

        # Full outer join preserves countries with only import or only export records for a month.
        trade_join_keys = ["country", "year", "month"]
        gold_trade_balance_by_country = (
            imports_df.join(exports_df, on=trade_join_keys, how="full_outer")
            .select(
                F.col("country"),
                F.coalesce(F.col("imports_value"), F.lit(0.0)).alias("imports_value"),
                F.coalesce(F.col("exports_value"), F.lit(0.0)).alias("exports_value"),
                (
                    F.coalesce(F.col("exports_value"), F.lit(0.0))
                    - F.coalesce(F.col("imports_value"), F.lit(0.0))
                ).alias("trade_balance_value"),
                F.col("year"),
                F.col("month"),
            )
            .where(F.col("country").isNotNull())
        )

        write_gold_table(
            gold_monthly_global_production,
            gold_uri,
            "gold_monthly_global_production",
            target_files_per_partition,
        )
        write_gold_table(
            gold_country_production_trend,
            gold_uri,
            "gold_country_production_trend",
            target_files_per_partition,
        )
        write_gold_table(
            gold_top_producers_by_month,
            gold_uri,
            "gold_top_producers_by_month",
            target_files_per_partition,
        )
        write_gold_table(
            gold_trade_balance_by_country,
            gold_uri,
            "gold_trade_balance_by_country",
            target_files_per_partition,
        )

        LOGGER.info("Gold table generation complete.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
