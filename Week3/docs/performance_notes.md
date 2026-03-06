# Performance Notes (Week 3 - Silver)

## Objective
Track Spark tuning evidence for the Silver layer on EMR Serverless.

## Implemented in `spark_to_silver.py`
- Writes Silver Parquet partitioned by `year`, `month`.
- Uses adaptive query execution via `spark.sql.adaptive.enabled`.
- Controls shuffle with `spark.sql.shuffle.partitions`.
- Reduces small files by `repartition(target_write_partitions, "year", "month")` before write.
- Controls write partition fan-out via `spark.target_files_per_partition`.

## Configs to Capture Per Run
- `spark.sql.adaptive.enabled`
- `spark.sql.shuffle.partitions`
- `spark.target_files_per_partition`
- EMR Serverless app release label
- Driver and executor resource settings (cores/memory/disk)

## Benchmark Template
Fill after each benchmark run.

| Metric | Baseline (Before Tuning) | Tuned (After Tuning) | Notes |
|---|---:|---:|---|
| Rows processed | TODO | TODO | |
| Runtime (mm:ss) | TODO | TODO | |
| Output file count (Silver) | TODO | TODO | |
| Avg file size (MB) | TODO | TODO | |
| Shuffle partitions | TODO | TODO | |
| AQE enabled | TODO | TODO | |

## EMR Serverless Resource Template
| Setting | Value |
|---|---|
| Application ID | `<YOUR_EMR_APPLICATION_ID>` |
| Release label | TODO |
| Driver | TODO |
| Executor | TODO |
| Max workers | TODO |

## Partition Pruning Proof Plan
1. Register Silver table in Glue/Athena (Step 4).
2. Run Athena query with explicit partition filters:
   - `WHERE year = 2024 AND month = 1`
3. Compare with a query that omits partition filters.
4. Capture scan bytes and runtime for both queries.
5. Save screenshots/query IDs in Week6 evidence.

## Validation SQL Draft (for Week 4)
```sql
SELECT COUNT(*)
FROM silver_jodi_oil
WHERE year = 2024
  AND month = 1;
```

```sql
SELECT COUNT(*)
FROM silver_jodi_oil;
```
