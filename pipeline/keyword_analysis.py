from pyspark.sql.functions import col

def keyword_frequency(word_df):

    word_filter = word_df.filter(
        (col("word") != "") &
        (~col("word").isin("the","and","to","a","of","is","in","on","for"))
    )

    word_count = word_filter.groupBy("word") \
        .count() \
        .orderBy("count", ascending=False)

    return word_count


def keyword_spending(join_spend_clean):

    word_spend = join_spend_clean.groupBy("word") \
        .sum("spend_amt") \
        .withColumnRenamed("sum(spend_amt)", "total_spend") \
        .orderBy("total_spend", ascending=False)

    return word_spend
