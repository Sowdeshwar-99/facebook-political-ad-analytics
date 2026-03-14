from pyspark.sql.functions import col, when, array, explode, split, lower

def clean_ads(datafile):

    ads = datafile.filter(
        (col("funding_entity").isNotNull()) &
        (col("page_name").isNotNull()) &
        ((col("ad_creative_bodies").isNotNull()) |
         (col("ad_creative_body").isNotNull()))
    )

    ads = ads.dropDuplicates(["id"])

    ads = ads.withColumn(
        "text_array",
        when(col("ad_creative_bodies").isNotNull(), col("ad_creative_bodies"))
        .otherwise(array(col("ad_creative_body")))
    )

    ads_flat = ads.select(
        "ad_creation_time",
        explode("text_array").alias("ad_text"),
        "page_name",
        "funding_entity",
        col("spend.upper_bound").cast("int").alias("spend_upper"),
        col("impressions.upper_bound").cast("int").alias("impressions_upper")
    )

    ads_clean = ads_flat.filter((col("ad_text") != "") & col("ad_text").isNotNull())

    return ads_clean


def tokenize_ads(ads_clean):

    ads_split = ads_clean.withColumn(
        "word",
        explode(split(lower(col("ad_text")), "\\W+"))
    )

    return ads_split
