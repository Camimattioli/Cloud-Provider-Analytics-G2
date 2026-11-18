def build_spark_session(config):
    from pyspark.sql import SparkSession

    spark_cfg = config.get("spark", {})
    app_name = spark_cfg.get("app_name", "CloudProviderAnalyticsG2-ETL")
    master = spark_cfg.get("master", "local[*]")
    timezone = spark_cfg.get("timezone", "UTC")
    shuffle_partitions = str(spark_cfg.get("shuffle_partitions", 8))
    adaptive_enabled = str(spark_cfg.get("adaptive_enabled", True)).lower()
    partition_overwrite_mode = spark_cfg.get("partition_overwrite_mode", "dynamic")
    broadcast_threshold = str(spark_cfg.get("broadcast_threshold_bytes", 10485760))

    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", timezone)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.adaptive.enabled", adaptive_enabled)
        .config("spark.sql.sources.partitionOverwriteMode", partition_overwrite_mode)
        .config("spark.sql.autoBroadcastJoinThreshold", broadcast_threshold)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
