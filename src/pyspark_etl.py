# Job de PySpark (Batch y Structured Streaming) para correr en Google Colab o entorno con Spark + Java.
# Lee con los StructType de src/schemas.py, aplica las reglas de Silver/Quarantine con funciones nativas
# de pyspark.sql.functions, usa F.broadcast() para organizaciones y recursos, y muestra los planes con explain().

from src.config_loader import load_pipeline_config, resolve_repo_path
from src.schemas import get_pyspark_schema
from src.spark_session import build_spark_session

CSV_SOURCES = {
    "customers_orgs": "customers_orgs.csv",
    "billing_monthly": "billing_monthly.csv",
    "resources": "resources.csv",
    "users": "users.csv",
    "support_tickets": "support_tickets.csv",
    "marketing_touches": "marketing_touches.csv",
    "nps_surveys": "nps_surveys.csv",
}


def run_pyspark_batch(spark, config):
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType, StringType, StructField, StructType

    raw_dir = resolve_repo_path(config["paths"]["raw_dir"], config)
    events_raw_dir = resolve_repo_path(config["paths"]["usage_events_raw_dir"], config)
    bronze_dir = resolve_repo_path(config["paths"]["bronze_dir"], config)
    silver_dir = resolve_repo_path(config["paths"]["silver_dir"], config)
    gold_dir = resolve_repo_path(config["paths"]["gold_dir"], config)
    quarantine_dir = resolve_repo_path(config["paths"]["quarantine_dir"], config)

    # 1. Bronze: eventos JSONL con esquema explícito (v1 + v2)
    events_schema = get_pyspark_schema("usage_events")
    raw_events = (
        spark.read.schema(events_schema)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(str(events_raw_dir / "*.jsonl"))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("event_ts", F.to_timestamp(F.col("timestamp")))
        .withColumn("year", F.year(F.col("event_ts")))
        .withColumn("month", F.month(F.col("event_ts")))
    )
    raw_events.write.mode("overwrite").partitionBy("year", "month").parquet(str(bronze_dir / "usage_events"))

    # 2. Bronze: 7 CSV
    bronze_csv = {}
    for name, filename in CSV_SOURCES.items():
        df = (
            spark.read.schema(get_pyspark_schema(name))
            .option("header", "true")
            .csv(str(raw_dir / filename))
            .withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
        )
        bronze_csv[name] = df

    # 3. Silver: dimensiones customers_orgs y resources (conservando res_ y org_)
    silver_orgs = (
        bronze_csv["customers_orgs"]
        .withColumn("org_id", F.trim(F.col("org_id")))
        .withColumn("org_name", F.trim(F.col("org_name")))
        .withColumn("plan", F.lower(F.trim(F.col("plan_tier"))))
        .withColumn("industry", F.lower(F.trim(F.col("industry"))))
        .withColumn("lifecycle_stage", F.lower(F.trim(F.col("lifecycle_stage"))))
        .withColumn(
            "nps_score",
            F.when((F.col("nps_score") >= -100.0) & (F.col("nps_score") <= 100.0), F.col("nps_score")).otherwise(
                F.lit(None).cast("double")
            ),
        )
    )

    tags_schema = StructType(
        [
            StructField("env", StringType(), True),
            StructField("pii", BooleanType(), True),
            StructField("cost_center", StringType(), True),
            StructField("costcenter", StringType(), True),
            StructField("team", StringType(), True),
            StructField("backup", StringType(), True),
        ]
    )
    silver_resources = (
        bronze_csv["resources"]
        .withColumn("resource_id", F.trim(F.col("resource_id")))
        .withColumn("org_id", F.trim(F.col("org_id")))
        .withColumn("service", F.lower(F.trim(F.col("service"))))
        .withColumn("region", F.lower(F.trim(F.col("region"))))
        .withColumn("state", F.lower(F.trim(F.col("state"))))
        .withColumn("_tags", F.from_json(F.col("tags_json"), tags_schema))
        .withColumn("env", F.coalesce(F.col("_tags.env"), F.lit("Sin Especificar")))
        .withColumn("pii", F.coalesce(F.col("_tags.pii"), F.lit(False)))
        .withColumn(
            "costcenter",
            F.coalesce(F.col("_tags.cost_center"), F.col("_tags.costcenter"), F.lit("Sin Asignar")),
        )
        .withColumn("team", F.coalesce(F.col("_tags.team"), F.lit("Sin Asignar")))
        .drop("tags_json", "_tags")
    )

    # 4. Silver y Quarantine de eventos
    quarantine_events = raw_events.filter(
        F.col("_corrupt_record").isNotNull() | (F.col("cost_usd_increment") < 0.0)
    ).withColumn("quarantine_reason", F.lit("NEGATIVE_COST_INCREMENT"))
    quarantine_events.write.mode("overwrite").partitionBy("year", "month").parquet(
        str(quarantine_dir / "usage_events_parquet")
    )

    valid_events = (
        raw_events.filter(F.col("_corrupt_record").isNull() & (F.col("cost_usd_increment") >= 0.0))
        .drop("_corrupt_record")
        .dropDuplicates(["event_id"])
        .withColumn("usage_date", F.to_date(F.col("event_ts")))
        .withColumn("year_month", F.date_format(F.col("event_ts"), "yyyy-MM"))
        .withColumn("is_value_null", F.col("value").isNull())
        .withColumn("is_unit_imputed", F.col("unit").isNull())
        .withColumn(
            "unit",
            F.when(F.col("unit").isNotNull(), F.col("unit"))
            .when(F.col("metric") == "cpu_hours", F.lit("hours"))
            .when(F.col("metric") == "requests", F.lit("count"))
            .when(F.col("metric") == "storage_gb_hours", F.lit("gb_hours"))
            .otherwise(F.lit("unknown")),
        )
    )

    w_sm = Window.partitionBy("service", "metric")
    valid_events = (
        valid_events.withColumn("_med", F.percentile_approx("cost_usd_increment", 0.5, 10000).over(w_sm))
        .withColumn("_abs_dev", F.abs(F.col("cost_usd_increment") - F.col("_med")))
        .withColumn("_mad", F.percentile_approx("_abs_dev", 0.5, 10000).over(w_sm))
        .withColumn(
            "mad_modified_zscore",
            F.when(F.col("_mad") > 1e-12, F.round(0.6745 * F.col("_abs_dev") / F.col("_mad"), 4)).otherwise(F.lit(0.0)),
        )
        .withColumn("is_mad_spike", F.col("mad_modified_zscore") > 3.5)
        .drop("_med", "_abs_dev", "_mad")
    )

    enriched_events = (
        valid_events.join(
            F.broadcast(silver_orgs.select("org_id", "org_name", "plan", "lifecycle_stage")),
            on="org_id",
            how="left",
        )
        .join(
            F.broadcast(silver_resources.select("resource_id", "env", "costcenter", "team")),
            on="resource_id",
            how="left",
        )
        .sortWithinPartitions("service", "org_id")
    )
    enriched_events.write.mode("overwrite").partitionBy("year", "month").parquet(str(silver_dir / "usage_events"))

    # 5. Gold: agregación diaria FinOps y plan físico real de Spark
    finops_daily = enriched_events.groupBy(
        "org_id", "org_name", "plan", "year_month", "usage_date", "year", "month", "service"
    ).agg(
        F.count("event_id").alias("events_count"),
        F.round(F.sum("cost_usd_increment"), 4).alias("total_cost_usd"),
        F.round(F.sum(F.coalesce(F.col("carbon_kg"), F.lit(0.0))), 4).alias("total_carbon_kg"),
        F.sum(F.coalesce(F.col("genai_tokens"), F.lit(0))).alias("total_genai_tokens"),
        F.sum(F.when(F.col("is_mad_spike"), 1).otherwise(0)).alias("mad_spike_count"),
    )
    finops_daily.explain(mode="formatted")
    finops_daily.write.mode("overwrite").partitionBy("year", "month").parquet(
        str(gold_dir / "finops_org_service_daily_parquet")
    )
    return finops_daily


def run_pyspark_streaming(spark, config, watermark_profile="replay_full"):
    from pyspark.sql import functions as F

    events_raw_dir = resolve_repo_path(config["paths"]["usage_events_raw_dir"], config)
    gold_dir = resolve_repo_path(config["paths"]["gold_dir"], config)
    quarantine_dir = resolve_repo_path(config["paths"]["quarantine_dir"], config)
    checkpoints_dir = resolve_repo_path(config["paths"]["checkpoints_dir"], config)

    max_files = int(config["streaming"].get("max_files_per_trigger", 10))
    delay_threshold = config["streaming"]["watermark_profiles"][watermark_profile]["delay_threshold"]

    raw_stream = (
        spark.readStream.schema(get_pyspark_schema("usage_events"))
        .option("maxFilesPerTrigger", max_files)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(str(events_raw_dir))
        .withColumn("event_ts", F.to_timestamp(F.col("timestamp")))
    )

    quarantine_stream = raw_stream.filter(
        F.col("_corrupt_record").isNotNull() | (F.col("cost_usd_increment") < 0.0)
    )
    q_query = (
        quarantine_stream.writeStream.format("parquet")
        .outputMode("append")
        .option("path", str(quarantine_dir / f"stream_quarantine_{watermark_profile}"))
        .option("checkpointLocation", str(checkpoints_dir / f"spark_quarantine_{watermark_profile}"))
        .trigger(availableNow=True)
        .start()
    )
    q_query.awaitTermination()

    valid_stream = (
        raw_stream.filter(F.col("_corrupt_record").isNull() & (F.col("cost_usd_increment") >= 0.0))
        .drop("_corrupt_record")
        .withWatermark("event_ts", delay_threshold)
        .dropDuplicatesWithinWatermark(["event_id"])
    )

    windowed_gold = valid_stream.groupBy(
        F.window(F.col("event_ts"), "1 day").alias("w"),
        F.col("org_id"),
        F.col("service"),
    ).agg(
        F.count("event_id").alias("events_count"),
        F.round(F.sum("cost_usd_increment"), 4).alias("total_cost_usd"),
    )

    g_query = (
        windowed_gold.writeStream.format("parquet")
        .outputMode("append")
        .option("path", str(gold_dir / f"stream_finops_{watermark_profile}"))
        .option("checkpointLocation", str(checkpoints_dir / f"spark_gold_{watermark_profile}"))
        .trigger(availableNow=True)
        .start()
    )
    g_query.awaitTermination()
    return g_query.lastProgress


if __name__ == "__main__":
    cfg = load_pipeline_config()
    sp = build_spark_session(cfg)
    run_pyspark_batch(sp, cfg)
    run_pyspark_streaming(sp, cfg, watermark_profile="replay_full")
