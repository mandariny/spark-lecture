from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_timestamp, max, min, mean, date_trunc, hour, minute, collect_set, count
if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("log dataframe ex") \
        .getOrCreate()

    log_data_inmemory = [
        ["130.31.184.234", "2023-02-26 04:15:21", "PATCH", "/users", "400", 61],
        ["28.252.170.12", "2023-02-26 04:15:21", "GET", "/events", "401", 73],
        ["180.97.92.48", "2023-02-26 04:15:22", "POST", "/parsers", "503", 17],
        ["73.218.61.17", "2023-02-26 04:16:22", "DELETE", "/lists", "201", 91],
        ["24.15.193.50", "2023-02-26 04:17:23", "PUT", "/auth", "400", 24]
    ]

    #  schema free available
    # df = ss.createDataFrame(log_data_inmemory)

    fields = StructType([
        StructField("ip", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("method", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("status_code", StringType(), False),
        StructField("latency", IntegerType(), False) # milliseconds
    ])

    # in-memory
    # df = ss.createDataFrame(log_data_inmemory, schema=fields)

    # file
    df = ss.read.schema(fields).csv("data/log.csv")

    # df.show()
    # df.printSchema()

    # a) column transform
    # a-1) latency : milliseconds -> seconds
    def milliseconds_to_seconds(latency):
        return latency / 1000

    df = df.withColumn(
        "latency_seconds",
        # milliseconds_to_seconds(col("latency"))
        milliseconds_to_seconds(df.latency)
    )

    # df.show()

    # a-2) StringType -> TimestampType
    df = df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp"))
    )

    # df.show()
    # df.printSchema()

    # b) filter
    # b-1) status_code = 400, endpoint = "/users"
    df.filter((df.status_code == "400") & (df.endpoint == "/users")) \
        # .show()
    df.where((df.status_code == "400") & (df.endpoint == "/users")) \
        # .show()

    # c) group by
    # c-1) max, min, mean latency by method, endpoint
    group_cols = ["method", "endpoint"]

    df.groupby(group_cols) \
        .agg(max("latency").alias("max_latency"),
             min("latency").alias("min_latency"),
             mean("latency").alias("mean_latency")).show()

    # c-2) remove duplicated ip list & count, group by minute
    group_cols = ["hour", "minute"]

    df.withColumn(
        "hour",
        hour(date_trunc("hour", col("timestamp")))
    ).withColumn(
        "minute",
        minute(date_trunc("minute", col("timestamp")))
    ).groupby(group_cols) \
        .agg(collect_set("ip").alias("ip_list"),
                              count("ip").alias("ip_count")).sort(group_cols) \
        .show()
        # .explain()
