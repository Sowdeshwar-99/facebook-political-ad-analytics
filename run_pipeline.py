from pipeline.ingestion import create_spark, load_ads_data
from pipeline.preprocessing import clean_ads, split_words
from pipeline.keyword_analysis import keyword_frequency
from pipeline.spend_analysis import top_investors
from pipeline.cpm_analysis import calculate_cpm

DATA_PATH = "hdfs:///data/ProjectDatasetFacebookAU/adsAU/*"

def main():

    spark = create_spark()

    df = load_ads_data(spark, DATA_PATH)

    ads_clean = clean_ads(df)

    words = split_words(ads_clean)

    word_counts = keyword_frequency(words)

    investors = top_investors(ads_clean)

    cpm_df = calculate_cpm(ads_clean)

    word_counts.show(20)
    investors.show(20)
    cpm_df.show(20)

if __name__ == "__main__":
    main()
