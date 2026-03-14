from pyspark.sql.functions import sum

def top_investors(ads_clean):

    investors = ads_clean.groupBy("funding_entity") \
        .sum("spend_upper") \
        .withColumnRenamed("sum(spend_upper)", "total_spend") \
        .orderBy("total_spend", ascending=False)

    return investors


def top_pages(ads_clean):

    pages = ads_clean.groupBy("page_name") \
        .sum("spend_upper") \
        .withColumnRenamed("sum(spend_upper)", "total_spend") \
        .orderBy("total_spend", ascending=False)

    return pages
