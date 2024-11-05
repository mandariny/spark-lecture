from pyspark.sql import SparkSession
from pyspark.sql.types import *

def load_data(ss: SparkSession, from_file, schema):
    if from_file:
        return ss.read.schema(schema).csv("data/log.csv")

    log_data_inmemory = [
        ["130.31.184.234", "2023-02-26 04:15:21", "PATCH", "/users", "400", 61],
        ["28.252.170.12", "2023-02-26 04:15:21", "GET", "/events", "401", 73],
        ["180.97.92.48", "2023-02-26 04:15:22", "POST", "/parsers", "503", 17],
        ["73.218.61.17", "2023-02-26 04:16:22", "DELETE", "/lists", "201", 91],
        ["24.15.193.50", "2023-02-26 04:17:23", "PUT", "/auth", "400", 24]
    ]

    return ss.createDataFrame(log_data_inmemory, schema)

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("log sql ex") \
        .getOrCreate()

    from_file = True

    fields = StructType([
        StructField("ip", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("method", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("status_code", StringType(), False),
        StructField("latency", IntegerType(), False) # milliseconds
    ])

    # load data & make view table
    table_name = "log_data"
    load_data(ss, from_file, fields).createOrReplaceTempView(table_name)

    # check data
    # ss.sql(f"SELECT * FROM {table_name}").show()

    # check schema
    # ss.sql(f"SELECT * FROM {table_name}").printSchema()

    # a) column transform
    # a-1) latency : milliseconds -> seconds
    ss.sql(f"""
        SELECT *, latency / 1000 AS latency_seconds
        FROM {table_name}
    """) \
    # .show()

    # a-2) StringType -> TimestampType
    ss.sql(f"""
        SELECT ip, TIMESTAMP(timestamp) AS timestamp, method, endpoint, status_code, latency
        FROM {table_name}
    """) \
    # .printSchema()

    # b) filter
    # b-1) status_code = 400, endpoint = "/users"
    ss.sql(f"""
        SELECT *
        FROM {table_name}
        WHERE status_code = '400' AND endpoint = '/users'
    """) \
    # .show()

    # c) group by
    # c-1) max, min, mean latency by method, endpoint
    ss.sql(f"""
        SELECT method, endpoint, 
            MAX(latency) AS max_latency,
            MIN(latency) AS min_latency,
            AVG(latency) AS mean_latency
        FROM {table_name}
        GROUP BY method, endpoint
    """) \
    # .show()

    # c-2) remove duplicated ip list & count, group by minute
    sql = f"""
        SELECT
            hour(date_trunc('HOUR', timestamp)) AS hour,
            minute(date_trunc('minute', timestamp)) AS minute,
            collect_set(ip) AS ip_list,
            count(ip) AS ip_count
        FROM {table_name}
        GROUP BY hour, minute
        ORDER BY hour, minute
    """

    ss.sql(sql).show()
    ss.sql(sql).explain()