from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f

# define schema
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                          StructField('UnitID', StringType(), True),
                          StructField('IncidentNumber', IntegerType(), True),
                          StructField('CallType', StringType(), True),
                          StructField('CallDate', StringType(), True),
                          StructField('WatchDate', StringType(), True),
                          StructField('CallFinalDisposition', StringType(), True),
                          StructField('AvailableDtTm', StringType(), True),
                          StructField('Address', StringType(), True),
                          StructField('City', StringType(), True),
                          StructField('Zipcode', IntegerType(), True),
                          StructField('Battalion', StringType(), True),
                          StructField('StationArea', StringType(), True),
                          StructField('Box', StringType(), True),
                          StructField('OriginalPriority', StringType(), True),
                          StructField('Priority', StringType(), True),
                          StructField('FinalPriority', IntegerType(), True),
                          StructField('ALSUnit', BooleanType(), True),
                          StructField('CallTypeGroup', StringType(), True),
                          StructField('NumAlarms', IntegerType(), True),
                          StructField('UnitType', StringType(), True),
                          StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                          StructField('FirePreventionDistrict', StringType(), True),
                          StructField('SupervisorDistrict', StringType(), True),
                          StructField('Neighborhood', StringType(), True),
                          StructField('Location', StringType(), True),
                          StructField('RowID', StringType(), True),
                          StructField('Delay', FloatType(), True)])

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("sf fire calls ex") \
        .getOrCreate()

    df = ss.read.schema(fire_schema).csv("data/sf-fire-calls.csv", header=True)

    # df.printSchema()
    # df.show()

    # Q-1) List all types of CallType calls that came in 2018 (CallDate) and find the most common type.
    df = df.withColumn("call_year",
                       f.year(f.to_timestamp("CallDate", "dd/MM/yyyy"))) \
        .filter(f.col("call_year") == "2018")

    # df.select(df.CallDate).show()

    df.select("CallType").where(f.col("CallType").isNotNull()) \
        .groupby("CallType") \
        .count().orderBy("count", ascending=False) \
        # .show(n=10, truncate=False)

    # Q-2) After checking the number of reports by month in 2018, check the month with the highest number of reports.
    df.withColumn("call_month",
                  f.month(f.to_timestamp("CallDate", "dd/MM/yyyy"))) \
        .groupby("call_month") \
        .count().orderBy("count", ascending=False) \
        # .show(n=10, truncate=False)

    # Q-3) Which San Francisco area received the most reports in 2018?
    df.filter(f.col("City") == "San Francisco") \
        .groupby("Address").count().orderBy("count", ascending=False) \
        # .show(n=10, truncate=False)

    # Q-4) Which five neighborhoods in San Francisco had the slowest response times in 2018?
    res = df.select("Neighborhood", "Delay") \
        .orderBy("Delay", ascending=False).take(5)

    # print(res)

    # Q-5) Save the 2018 data in parquet format and then import it back.
    # write
    df.write.format("parquet").mode("overwrite").save("data/2018-sf-fire-calls.parquet")

    # read
    parquet_df = ss.read.format("parquet").parquet("data/2018-sf-fire-calls.parquet")

    parquet_df.printSchema()