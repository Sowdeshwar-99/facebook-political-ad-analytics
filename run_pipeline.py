from pipeline.ingestion import create_spark, load_ads_data
from pipeline.preprocessing import clean_ads, tokenize_ads
from pipeline.keyword_analysis import keyword_frequency
from pipeline.spend_analysis import top_investors
from pipeline.cpm_analysis import calculate_cpm

DATA_PATH = "hdfs:///data/ProjectDatasetFacebookAU/*"

def main():

    spark = create_spark()

    df = load_ads_data(spark, DATA_PATH)

    ads_clean = clean_ads(df)

    words = tokenize_ads(ads_clean)

    word_counts = keyword_frequency(words)

    investors = top_investors(ads_clean)

    cpm_df = calculate_cpm(ads_clean)

    word_counts.show()
    investors.show()
    cpm_df.show()

if __name__ == "__main__":
    main()
