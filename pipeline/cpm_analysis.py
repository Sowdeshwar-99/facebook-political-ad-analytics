from pyspark.sql.functions import col

def calculate_cpm(ads_clean):

    reach_df = ads_clean.select(
        "ad_text",
        "funding_entity",
        "page_name",
        col("spend_upper").cast("double").alias("spend"),
        col("impressions_upper").cast("double").alias("impressions")
    ).filter(
        col("spend").isNotNull() &
        col("impressions").isNotNull() &
        (col("impressions") > 0)
    )

    reach_df = reach_df.withColumn(
        "cpm",
        (col("spend") / col("impressions")) * 1000
    )

    return reach_df
