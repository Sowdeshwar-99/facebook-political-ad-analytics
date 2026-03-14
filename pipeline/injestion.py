from pyspark.sql import SparkSession

def create_spark():
    return SparkSession.builder.appName("FacebookAdsAnalytics").getOrCreate()

def load_ads_data(spark, path):
    df = spark.read.option("multiLine", "false").json(path)
    return df
