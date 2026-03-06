import argparse
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LOGGER = logging.getLogger("data_quality_spark")
ENV_VAR_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")
GOLD_TABLES = [
    "gold_monthly_global_production",
    "gold_country_production_trend",
    "gold_top_producers_by_month",
    "gold_trade_balance_by_country",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run data quality checks on Silver/Gold S3 datasets and write JSON report to S3."
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config file (default: config.yaml).",
    )
    parser.add_argument(
        "--report-uri",
        default="",
        help="Optional explicit report URI (for example s3a://<bucket>/jodi-oil/reports/).",
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


def add_check(
    checks: List[Dict],
    name: str,
    passed: bool,
    expected: str,
    actual,
    details: str = "",
    severity: str = "error",
) -> None:
    checks.append(
        {
            "check_name": name,
            "status": "PASS" if passed else "FAIL",
            "severity": severity,
            "expected": expected,
            "actual": str(actual),
            "details": details,
        }
    )


def derive_reports_uri(raw_uri: str, gold_uri: str, explicit_report_uri: str, config: Dict) -> str:
    candidates = [
        explicit_report_uri.strip(),
        os.getenv("REPORTS_URI", "").strip(),
        str(config.get("reports_uri", "")).strip(),
    ]

    for candidate in candidates:
        if candidate:
            if "${" in candidate:
                raise ValueError(
                    "Unresolved placeholder in reports URI. Set REPORTS_URI or pass --report-uri."
                )
            return candidate

    if "/raw/" in raw_uri:
        return raw_uri.replace("/raw/", "/reports/")

    if "/gold/" in gold_uri:
        return gold_uri.replace("/gold/", "/reports/")

    raise ValueError(
        "Could not derive reports URI. Provide REPORTS_URI in .env or pass --report-uri."
    )


def read_parquet_safe(spark: SparkSession, path: str, label: str, checks: List[Dict]) -> Optional[DataFrame]:
    try:
        LOGGER.info("Reading %s from %s", label, path)
        df = spark.read.parquet(path.rstrip("/"))
        if not df.columns:
            raise ValueError(f"{label} has no columns at path: {path}")
        add_check(
            checks,
            f"{label}_readable",
            True,
            "Parquet path readable",
            "read_ok",
        )
        return df
    except Exception as exc:
        add_check(
            checks,
            f"{label}_readable",
            False,
            "Parquet path readable",
            "read_failed",
            details=str(exc)[:1000],
        )
        return None


def run_silver_checks(silver_df: DataFrame, checks: List[Dict]) -> None:
    silver_count = silver_df.count()
    add_check(checks, "silver_non_empty", silver_count > 0, "> 0 rows", silver_count)

    has_partitions = {"year", "month"}.issubset(set(silver_df.columns))
    add_check(
        checks,
        "silver_has_partition_columns",
        has_partitions,
        "columns year and month exist",
        str(sorted(silver_df.columns)),
    )
    if not has_partitions:
        return

    invalid_partition_rows = silver_df.filter(
        F.col("year").isNull()
        | F.col("month").isNull()
        | (F.col("month") < 1)
        | (F.col("month") > 12)
    ).count()
    add_check(
        checks,
        "silver_valid_year_month_values",
        invalid_partition_rows == 0,
        "0 invalid partition rows",
        invalid_partition_rows,
    )

    partition_count = silver_df.select("year", "month").distinct().count()
    add_check(
        checks,
        "silver_has_partitions",
        partition_count > 0,
        "> 0 distinct year-month partitions",
        partition_count,
    )


def run_gold_checks(gold_dfs: Dict[str, DataFrame], checks: List[Dict]) -> None:
    add_check(
        checks,
        "gold_tables_available",
        len(gold_dfs) == len(GOLD_TABLES),
        f"{len(GOLD_TABLES)} readable gold tables",
        len(gold_dfs),
    )

    for table_name, df in gold_dfs.items():
        table_count = df.count()
        add_check(
            checks,
            f"{table_name}_non_empty",
            table_count > 0,
            "> 0 rows",
            table_count,
        )

        has_partitions = {"year", "month"}.issubset(set(df.columns))
        add_check(
            checks,
            f"{table_name}_has_partition_columns",
            has_partitions,
            "columns year and month exist",
            str(sorted(df.columns)),
        )

    monthly_df = gold_dfs.get("gold_monthly_global_production")
    if monthly_df is not None and {"metric_name", "year", "month"}.issubset(set(monthly_df.columns)):
        monthly_duplicates = (
            monthly_df.groupBy("metric_name", "year", "month")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        add_check(
            checks,
            "gold_monthly_global_production_unique_key",
            monthly_duplicates == 0,
            "0 duplicate (metric_name, year, month)",
            monthly_duplicates,
        )

    top_df = gold_dfs.get("gold_top_producers_by_month")
    if top_df is not None and {"producer_rank", "year", "month"}.issubset(set(top_df.columns)):
        invalid_ranks = top_df.filter(
            F.col("producer_rank").isNull()
            | (F.col("producer_rank") < 1)
            | (F.col("producer_rank") > 10)
        ).count()
        add_check(
            checks,
            "gold_top_producers_rank_range",
            invalid_ranks == 0,
            "producer_rank between 1 and 10",
            invalid_ranks,
        )

        duplicate_rank_rows = (
            top_df.groupBy("year", "month", "producer_rank")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        add_check(
            checks,
            "gold_top_producers_unique_rank_per_month",
            duplicate_rank_rows == 0,
            "0 duplicate ranks per (year, month)",
            duplicate_rank_rows,
        )

    trade_df = gold_dfs.get("gold_trade_balance_by_country")
    if trade_df is not None and {
        "imports_value",
        "exports_value",
        "trade_balance_value",
    }.issubset(set(trade_df.columns)):
        mismatch_rows = trade_df.filter(
            F.abs(
                (
                    F.coalesce(F.col("exports_value"), F.lit(0.0))
                    - F.coalesce(F.col("imports_value"), F.lit(0.0))
                )
                - F.coalesce(F.col("trade_balance_value"), F.lit(0.0))
            )
            > F.lit(1e-6)
        ).count()
        add_check(
            checks,
            "gold_trade_balance_formula_consistent",
            mismatch_rows == 0,
            "exports_value - imports_value = trade_balance_value",
            mismatch_rows,
        )


def write_report_json(
    spark: SparkSession,
    reports_uri: str,
    report_payload: Dict,
    run_date: str,
    run_ts: str,
) -> str:
    output_path = (
        reports_uri.rstrip("/")
        + f"/data_quality_report/run_date={run_date}/run_ts={run_ts}"
    )
    report_json = json.dumps(report_payload, ensure_ascii=True)

    report_df = spark.createDataFrame([(report_json,)], ["value"])
    report_df.coalesce(1).write.mode("overwrite").text(output_path)

    LOGGER.info("Data quality report written to %s", output_path)
    return output_path


def main() -> None:
    configure_logging()
    args = parse_args()

    config = load_config(args.config)
    mode = str(config.get("mode", "")).strip().lower()
    if mode and mode != "aws":
        LOGGER.warning("Config mode is '%s'. Expected 'aws' for this pipeline.", mode)

    dataset_name = str(config.get("dataset_name", "unknown_dataset")).strip() or "unknown_dataset"
    raw_uri = get_required_string(config, ["raw_uri"])
    silver_uri = get_required_string(config, ["silver_uri"])
    gold_uri = get_required_string(config, ["gold_uri"])
    reports_uri = derive_reports_uri(raw_uri, gold_uri, args.report_uri, config)

    adaptive_enabled = parse_bool(
        get_nested(config, ["spark", "adaptive_enabled"]),
        "spark.adaptive_enabled",
    )
    shuffle_partitions = parse_positive_int(
        get_nested(config, ["spark", "shuffle_partitions"]),
        "spark.shuffle_partitions",
    )

    spark = (
        SparkSession.builder.appName("jodi_oil_data_quality")
        .config("spark.sql.adaptive.enabled", str(adaptive_enabled).lower())
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .getOrCreate()
    )

    checks: List[Dict] = []
    utc_now = datetime.now(timezone.utc)
    generated_at_utc = utc_now.strftime("%Y-%m-%dT%H:%M:%SZ")
    run_date = utc_now.strftime("%Y-%m-%d")
    run_ts = utc_now.strftime("%Y%m%dT%H%M%SZ")

    try:
        silver_df = read_parquet_safe(spark, silver_uri, "silver", checks)
        if silver_df is not None:
            run_silver_checks(silver_df, checks)

        gold_dfs: Dict[str, DataFrame] = {}
        for table_name in GOLD_TABLES:
            table_path = gold_uri.rstrip("/") + f"/{table_name}"
            df = read_parquet_safe(spark, table_path, table_name, checks)
            if df is not None:
                gold_dfs[table_name] = df

        run_gold_checks(gold_dfs, checks)

        failed_count = sum(1 for check in checks if check["status"] == "FAIL")
        passed_count = sum(1 for check in checks if check["status"] == "PASS")

        report_payload = {
            "dataset_name": dataset_name,
            "mode": mode or "aws",
            "generated_at_utc": generated_at_utc,
            "input_paths": {
                "raw_uri": raw_uri,
                "silver_uri": silver_uri,
                "gold_uri": gold_uri,
                "reports_uri": reports_uri,
            },
            "summary": {
                "total_checks": len(checks),
                "passed_checks": passed_count,
                "failed_checks": failed_count,
            },
            "checks": checks,
        }

        report_output_path = write_report_json(
            spark=spark,
            reports_uri=reports_uri,
            report_payload=report_payload,
            run_date=run_date,
            run_ts=run_ts,
        )

        if failed_count > 0:
            raise RuntimeError(
                f"Data quality failed: {failed_count} check(s) failed. Report path: {report_output_path}"
            )

        LOGGER.info("Data quality passed. All checks succeeded.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
