from pyspark.sql import SparkSession

def create_spark():
    spark = SparkSession.builder.appName(
        "Facebook Political Ads Analytics"
    ).getOrCreate()
    return spark


def load_ads_data(spark, path):
    df = spark.read.option(
        "multiLine", "false"
    ).json(path)

    df.cache()

    return df
