from pyspark.sql.functions import lower, col, sum

def covid_ads(ads_clean):

    ads_covid = ads_clean.filter(
        lower(col("ad_text")).contains("covid")
    )

    return ads_covid


def covid_spending(ads_covid):

    return ads_covid.agg(
        sum("spend_upper").alias("covid_spend")
    )
