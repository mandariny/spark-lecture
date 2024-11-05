from pyspark.sql import SparkSession
from pyspark.sql.types import *

def load_user_visits(ss: SparkSession):
    schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("visits", IntegerType(), False),
    ])

    data = [
        (1, 10),
        (2, 27),
        (3, 2),
        (4, 5),
        (5, 88),
        (6, 1),
        (7, 5)
    ]

    return ss.createDataFrame(data, schema)

def load_user_name(ss: SparkSession):
    schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("name", StringType(), False)
    ])

    data = [
        (1, "Andrew"),
        (2, "Chris"),
        (3, "John"),
        (4, "Bob"),
        (6, "Ryan"),
        (7, "Mali"),
        (8, "Tony")
    ]

    return ss.createDataFrame(data, schema)

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("dataframe join ex") \
        .getOrCreate()

    user_visits_df = load_user_visits(ss)
    user_names_df = load_user_name(ss)

    # user_visits_df.show()
    # user_names_df.show()

    # 1) default join = cartesian(cross) join (row : M * N) -> performance low! not good,,
    user_names_df.join(user_visits_df) \
        # .show()

    # 2) inner join
    user_names_df.join(user_visits_df, on = "user_id") \
    # user_names_df.join(user_visits_df, on="user_id", how="inner") \
    # .show()

    # 3) left outer join
    user_names_df.join(user_visits_df, on="user_id", how="left") \
    # .show()

    # 4) right outer join
    user_names_df.join(user_visits_df, on="user_id", how="right") \
    # .show()

    # 5) full outer join
    user_names_df.join(user_visits_df, on="user_id", how="outer") \
    .show()