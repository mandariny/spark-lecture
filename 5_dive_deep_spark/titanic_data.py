from pyspark.sql import SparkSession

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("spark_jobs_ex") \
        .getOrCreate()

    # survived male passenger
    # mean fare by pclass

    df = ss.read.option("header", "true")\
        .option("inferSchema", "true")\
        .csv("data/titanic_data.csv")

    df = df.repartition(5).where("Sex = 'male'")\
        .select("Survived", "Pclass", "Fare")\
        .groupby("Survived", "Pclass")\
        .mean()

    print(f"result ==> {df.collect()}")

    df.explain(mode="extended")

    # while True:
    #     pass