"""
main.py

Entry point for the PySpark data pipeline. Reads data, cleans it,
performs feature engineering, runs analysis, and saves outputs.
"""

from DataProcessor import *


def main():
    spark = create_spark_session()
    INPUT_PATH = "src/data/StudentDepressionDataset.csv"
    OUTPUT_PATH = "output"


    raw_df = load_data(spark, INPUT_PATH)

    cleaned_df = clean_data(raw_df)
    report_data_quality(cleaned_df)


    processed_path = f"{OUTPUT_PATH}/processed_data.parquet"
    save_parquet(cleaned_df, processed_path)

    feat_df = feature_engineering(cleaned_df)
    feat_path = f"{OUTPUT_PATH}/feature_engineered_data.parquet"
    save_parquet(feat_df, feat_path)


    dep_by_demo_df, cgpa_by_sleep_df = distribution_analysis(feat_df)
    save_parquet(dep_by_demo_df, f"{OUTPUT_PATH}/distributions/depression_by_demographics.parquet")
    save_parquet(cgpa_by_sleep_df, f"{OUTPUT_PATH}/distributions/academic_performance.parquet")


    corr_matrix_df, top5_dep_corr_df = correlation_analysis(feat_df)
    save_parquet(corr_matrix_df, f"{OUTPUT_PATH}/correlations/correlation_matrix.parquet")
    save_parquet(top5_dep_corr_df, f"{OUTPUT_PATH}/correlations/depression_correlations.parquet")


    city_degree_df, demo_stress_df, sleep_perf_df = aggregations(feat_df)
    save_parquet(city_degree_df, f"{OUTPUT_PATH}/aggregations/city_degree_stats.parquet")
    save_parquet(demo_stress_df, f"{OUTPUT_PATH}/aggregations/demographic_stress.parquet")
    save_parquet(sleep_perf_df, f"{OUTPUT_PATH}/aggregations/sleep_performance.parquet")

    high_risk_df = risk_analysis(feat_df)
    save_parquet(high_risk_df, f"{OUTPUT_PATH}/risk_analysis/high_risk_students.parquet")

    spark.stop()


if __name__ == "__main__":
    main()
