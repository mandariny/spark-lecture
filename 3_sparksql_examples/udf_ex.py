from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("log dataframe ex") \
        .getOrCreate()

    # 1. define udf functions

    def add_prefix(string):
        return "prefix_" + string

    def cube(n):
        return n ** 3

    # 2. prepare data
    schema = StructType([
        StructField("name", StringType(), False),
        StructField("id", IntegerType(), False),
    ])
    data = [
        ["abc", 1],
        ["ffff", 2],
        ["cccc", 3],
    ]

    df = ss.createDataFrame(data, schema=schema)

    # 3. use dataframe api
    add_prefix_udf = udf(lambda s: add_prefix(s), StringType())
    cube_udf = udf(lambda n: cube(n), LongType())

    df.select(
        add_prefix_udf(col("name")).alias("prefix_name"),
        cube_udf(col("id")).alias("cube_id")
    ).show()

    # 4. sql api
    ss.udf.register("add_prefix", add_prefix, StringType())
    ss.udf.register("cube", cube, LongType())

    df.createOrReplaceTempView("udf_test")

    # ss.sql("""
    #     SELECT
    #         add_prefix(name) AS prefix_name,
    #         cube(id) AS cube_id
    #     FROM udf_test
    # """).show()

    # The order of execution is not guaranteed
    # Null Check Caution
    ss.sql("""
        SELECT
            name
        FROM udf_test
        WHERE name IS NOT NULL AND length(add_prefix(name)) > 10
    """).explain()