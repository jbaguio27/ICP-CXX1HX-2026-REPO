import argparse
import logging
import os
import re
from pathlib import Path
from typing import Dict, Iterable, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LOGGER = logging.getLogger("spark_to_silver")
ENV_VAR_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read raw JODI CSV files from S3 and write Silver Parquet partitioned by year/month."
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


def normalize_column_name(name: str) -> str:
    normalized = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "col"


def unique_normalized_names(columns: Iterable[str]) -> list[str]:
    counts: Dict[str, int] = {}
    result = []

    for original in columns:
        base = normalize_column_name(original)
        index = counts.get(base, 0)
        counts[base] = index + 1
        result.append(base if index == 0 else f"{base}_{index + 1}")

    return result


def normalize_columns(df: DataFrame) -> DataFrame:
    new_names = unique_normalized_names(df.columns)

    if list(df.columns) != new_names:
        LOGGER.info("Normalizing %s input columns to snake_case.", len(df.columns))

    return df.toDF(*new_names)


def month_to_int(column_expr):
    month_map = {
        "JAN": 1,
        "JANUARY": 1,
        "FEB": 2,
        "FEBRUARY": 2,
        "MAR": 3,
        "MARCH": 3,
        "APR": 4,
        "APRIL": 4,
        "MAY": 5,
        "JUN": 6,
        "JUNE": 6,
        "JUL": 7,
        "JULY": 7,
        "AUG": 8,
        "AUGUST": 8,
        "SEP": 9,
        "SEPT": 9,
        "SEPTEMBER": 9,
        "OCT": 10,
        "OCTOBER": 10,
        "NOV": 11,
        "NOVEMBER": 11,
        "DEC": 12,
        "DECEMBER": 12,
    }

    month_text = F.upper(F.trim(column_expr.cast("string")))
    month_int = F.when(month_text.rlike(r"^[0-9]{1,2}$"), month_text.cast("int"))

    for key, value in month_map.items():
        month_int = month_int.when(month_text == F.lit(key), F.lit(value))

    return month_int


def derive_year_month_from_text(column_expr) -> Tuple:
    text = F.trim(column_expr.cast("string"))
    normalized = F.regexp_replace(text, r"[./]", "-")
    digits_only = F.regexp_replace(normalized, r"[^0-9]", "")
    date_prefix = F.regexp_extract(
        normalized,
        r"^([0-9]{4}-[0-9]{1,2}-[0-9]{1,2})",
        1,
    )

    timestamp_value = F.coalesce(
        F.to_timestamp(normalized, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(normalized, "yyyy-MM-dd HH:mm:ss.SSS"),
        F.to_timestamp(normalized, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(normalized, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
    )

    full_date = F.coalesce(
        F.to_date(timestamp_value),
        F.to_date(date_prefix, "yyyy-MM-dd"),
        F.to_date(date_prefix, "yyyy-M-d"),
        F.to_date(date_prefix, "M-d-yyyy"),
        F.to_date(date_prefix, "d-M-yyyy"),
    )

    month_name_date = F.coalesce(
        F.to_date(F.concat(normalized, F.lit(" 01")), "MMM yyyy dd"),
        F.to_date(F.concat(normalized, F.lit(" 01")), "MMMM yyyy dd"),
        F.to_date(F.concat(normalized, F.lit(" 01")), "yyyy MMM dd"),
        F.to_date(F.concat(normalized, F.lit(" 01")), "yyyy MMMM dd"),
    )

    year_month_year = F.regexp_extract(
        normalized,
        r"^(19\d{2}|20\d{2})[- ]?(0?[1-9]|1[0-2])$",
        1,
    ).cast("int")
    year_month_month = F.regexp_extract(
        normalized,
        r"^(19\d{2}|20\d{2})[- ]?(0?[1-9]|1[0-2])$",
        2,
    ).cast("int")

    yyyymm_year = F.regexp_extract(
        digits_only,
        r"^(19\d{2}|20\d{2})(0[1-9]|1[0-2])$",
        1,
    ).cast("int")
    yyyymm_month = F.regexp_extract(
        digits_only,
        r"^(19\d{2}|20\d{2})(0[1-9]|1[0-2])$",
        2,
    ).cast("int")

    year_col = F.coalesce(
        F.year(full_date),
        F.year(month_name_date),
        year_month_year,
        yyyymm_year,
    )
    month_col = F.coalesce(
        F.month(full_date),
        F.month(month_name_date),
        year_month_month,
        yyyymm_month,
    )

    return year_col, month_col


def add_partition_columns(df: DataFrame) -> DataFrame:
    available = set(df.columns)

    year_candidates = ["year", "yr", "calendar_year", "ref_year"]
    month_candidates = ["month", "mo", "mnth", "calendar_month", "ref_month"]
    period_candidates = [
        "period",
        "time_period",
        "reference_period",
        "month_year",
        "date",
        "time",
    ]

    year_col_name = next((c for c in year_candidates if c in available), None)
    month_col_name = next((c for c in month_candidates if c in available), None)
    period_col_name = next((c for c in period_candidates if c in available), None)

    LOGGER.info(
        "Deriving partition columns using year=%s month=%s period=%s",
        year_col_name,
        month_col_name,
        period_col_name,
    )

    year_col = F.lit(None).cast("int")
    month_col = F.lit(None).cast("int")

    if year_col_name:
        year_col = F.coalesce(year_col, F.col(year_col_name).cast("int"))

    if month_col_name:
        month_col = F.coalesce(month_col, month_to_int(F.col(month_col_name)))

    if period_col_name:
        derived_year, derived_month = derive_year_month_from_text(F.col(period_col_name))
        year_col = F.coalesce(year_col, derived_year)
        month_col = F.coalesce(month_col, derived_month)

    file_year, file_month = derive_year_month_from_text(F.col("source_file_name"))
    year_col = F.coalesce(year_col, file_year)
    month_col = F.coalesce(month_col, file_month)

    return df.withColumn("year", year_col).withColumn("month", month_col)


def validate_partition_columns(df: DataFrame) -> None:
    invalid_condition = (
        F.col("year").isNull()
        | F.col("month").isNull()
        | (F.col("year") < 1900)
        | (F.col("year") > 2100)
        | (F.col("month") < 1)
        | (F.col("month") > 12)
    )

    invalid_df = df.filter(invalid_condition)
    invalid_count = invalid_df.count()

    if invalid_count == 0:
        return

    sample_files = [
        row["source_file_name"]
        for row in invalid_df.select("source_file_name").distinct().limit(5).collect()
    ]

    raise ValueError(
        "Failed to derive valid year/month for all rows. "
        f"Invalid rows: {invalid_count}. Example source files: {sample_files}"
    )


def main() -> None:
    configure_logging()
    args = parse_args()

    config = load_config(args.config)
    mode = str(config.get("mode", "")).strip().lower()
    if mode and mode != "aws":
        LOGGER.warning("Config mode is '%s'. Expected 'aws' for this pipeline.", mode)

    raw_uri = get_required_string(config, ["raw_uri"])
    silver_uri = get_required_string(config, ["silver_uri"])

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
        SparkSession.builder.appName("jodi_oil_spark_to_silver")
        .config("spark.sql.adaptive.enabled", str(adaptive_enabled).lower())
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    try:
        input_path = raw_uri.rstrip("/") + "/*.csv"
        LOGGER.info("Reading raw CSV files from %s", input_path)

        raw_df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(input_path)
        )

        if not raw_df.columns:
            raise ValueError(f"No columns detected in raw input: {input_path}")

        silver_df = normalize_columns(raw_df)
        silver_df = silver_df.withColumn("source_file_name", F.input_file_name())
        silver_df = silver_df.withColumn("ingested_at_utc", F.current_timestamp())

        silver_df = add_partition_columns(silver_df)
        validate_partition_columns(silver_df)

        row_count = silver_df.count()
        distinct_partition_count = silver_df.select("year", "month").distinct().count()
        target_write_partitions = max(1, distinct_partition_count * target_files_per_partition)

        LOGGER.info("Rows processed: %s", row_count)
        LOGGER.info("Distinct year-month partitions: %s", distinct_partition_count)
        LOGGER.info("Target write partitions: %s", target_write_partitions)

        write_df = silver_df.repartition(target_write_partitions, "year", "month")

        LOGGER.info("Writing Silver Parquet to %s", silver_uri)
        (
            write_df.write.mode("overwrite")
            .partitionBy("year", "month")
            .parquet(silver_uri)
        )

        LOGGER.info("Silver write complete.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

