import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, when, lit, avg, udf
from pyspark.sql.types import FloatType
from src.DataProcessor import (
    create_spark_session,
    load_data,
    clean_data,
    feature_engineering,
    distribution_analysis,
    correlation_analysis,
    aggregations,
    risk_analysis,
    parse_sleep_duration,
    schema,
)

@pytest.fixture(scope="session")
def spark():
    """
    Creates a Spark session for all tests in this file.
    Will be torn down after the test session finishes.
    """
    spark_session = create_spark_session(app_name="TestStudentMentalHealth")
    yield spark_session
    spark_session.stop()

def test_load_data(spark):
    """
    Test that load_data correctly loads a small CSV with the given schema.
    """

    test_csv_path = "tests/test_data.csv"

    df = load_data(spark, test_csv_path)

    assert df.count() == 2

    expected_columns = set(schema.fieldNames())
    actual_columns = set(df.columns)
    assert expected_columns == actual_columns

def test_clean_data(spark):

    sample_data = [
        Row(id=1, Gender="Male", Age=20.0, City="CityA", Profession="Student",
            **{"Academic Pressure": 5.0, "Work Pressure": 4.0, "CGPA": 3.1, 
               "Study Satisfaction": 4.5, "Job Satisfaction": 3.0, 
               "Sleep Duration": "7", "Dietary Habits": "Veg", "Degree": "Bachelor", 
               "Have you ever had suicidal thoughts ?": "No", 
               "Work/Study Hours": 5.0, "Financial Stress": 3.0,
               "Family History of Mental Illness": "No", "Depression": 2}),
        Row(id=2, Gender="Female", Age=-1.0, City="CityB", Profession="Intern",
            **{"Academic Pressure": 3.0, "Work Pressure": 2.0, "CGPA": 3.5, 
               "Study Satisfaction": 3.0, "Job Satisfaction": 4.0, 
               "Sleep Duration": "25", "Dietary Habits": "Non-Veg", "Degree": "Bachelor", 
               "Have you ever had suicidal thoughts ?": "Yes", 
               "Work/Study Hours": 6.0, "Financial Stress": 5.0,
               "Family History of Mental Illness": "Yes", "Depression": 4}),
        Row(id=3, Gender="Male", Age=18.0, City="CityC", Profession="Engineer",
            **{"Academic Pressure": 6.0, "Work Pressure": 3.0, "CGPA": 2.8, 
               "Study Satisfaction": 4.0, "Job Satisfaction": 2.0, 
               "Sleep Duration": None, "Dietary Habits": "Veg", "Degree": "Master", 
               "Have you ever had suicidal thoughts ?": "No", 
               "Work/Study Hours": 4.0, "Financial Stress": 2.0,
               "Family History of Mental Illness": "No", "Depression": 3}),
    ]
    df_dirty = spark.createDataFrame(sample_data)

    df_clean = clean_data(df_dirty)

    assert df_clean.count() == 1

    assert dict(df_clean.dtypes)["Sleep Duration"] == "float"
    row = df_clean.collect()[0]
    assert row.Age == 20.0
    assert row["Sleep Duration"] == 7.0

def test_feature_engineering(spark):
    sample_data = [
        Row(id=1, Gender="Male", Age=20.0, City="CityA", Profession="Student",
            **{"Academic Pressure": 6.0, "Work Pressure": 3.0, "Financial Stress": 9.0,
               "CGPA": 3.0, "Depression": 2, "Sleep Duration": 7.0}),
        Row(id=2, Gender="Female", Age=24.0, City="CityB", Profession="Intern",
            **{"Academic Pressure": 4.0, "Work Pressure": 2.0, "Financial Stress": 6.0,
               "CGPA": 3.5, "Depression": 4, "Sleep Duration": 5.0}),
    ]
    df_in = spark.createDataFrame(sample_data)

    df_out = feature_engineering(df_in)

    assert "stress_index" in df_out.columns
    assert "sleep_category" in df_out.columns
    assert "age_group" in df_out.columns
    assert "CGPA_normalized" in df_out.columns
    assert "Depression_normalized" in df_out.columns
    assert "stress_index_normalized" in df_out.columns

    row1 = df_out.filter(col("id") == 1).collect()[0]
    assert row1["stress_index"] == 6.0

    assert row1["sleep_category"] == "Normal"
    row2 = df_out.filter(col("id") == 2).collect()[0]
    assert row2["sleep_category"] == "Low"

    assert row1["age_group"] == "18-21"
    assert row2["age_group"] == "22-25"

    assert "Gender_Male" in df_out.columns
    assert "Gender_Female" in df_out.columns
    assert row1["Gender_Male"] == 1
    assert row1["Gender_Female"] == 0

    assert row1["CGPA_normalized"] == 0.0
    assert row2["CGPA_normalized"] == 1.0


def test_distribution_analysis(spark):
    sample_data = [
        Row(Age=20.0, Profession="Student", Depression=2, CGPA=3.5, sleep_category="Normal"),
        Row(Age=22.0, Profession="Intern", Depression=4, CGPA=3.0, sleep_category="Low"),
        Row(Age=25.0, Profession="Student", Depression=3, CGPA=3.7, sleep_category="High"),
    ]
    df = spark.createDataFrame(sample_data)
    dep_by_demo, cgpa_by_sleep = distribution_analysis(df)


    assert set(dep_by_demo.columns) == {"age_group", "Profession", "avg_depression"}

    assert set(cgpa_by_sleep.columns) == {"sleep_category", "avg_cgpa", "stddev_cgpa"}

def test_correlation_analysis(spark):
    sample_data = [
        Row(Depression=2, CGPA=3.5, Age=20.0, stress_index=5.0),
        Row(Depression=4, CGPA=2.9, Age=22.0, stress_index=7.0),
        Row(Depression=3, CGPA=3.2, Age=25.0, stress_index=6.0),
    ]
    df = spark.createDataFrame(sample_data)
    corr_df, top5_df = correlation_analysis(df)

    assert set(corr_df.columns) == {"column1", "column2", "correlation"}

    assert "Depression" in top5_df.select("column1").collect()[0] or \
           "Depression" in top5_df.select("column2").collect()[0]

    numeric_cols = ["Depression", "CGPA", "Age", "stress_index"]
    expected_pairs = 6
    assert corr_df.count() == expected_pairs


def test_aggregations(spark):
    sample_data = [
        Row(City="CityA", Degree="Bachelor", Depression=2, stress_index=4.0, Age=20.0, Gender="Male", CGPA=3.2, **{"Academic Pressure": 5.0}),
        Row(City="CityA", Degree="Bachelor", Depression=3, stress_index=5.0, Age=21.0, Gender="Female", CGPA=3.1, **{"Academic Pressure": 4.0}),
        Row(City="CityB", Degree="Master",   Depression=4, stress_index=6.0, Age=22.0, Gender="Female", CGPA=3.5, **{"Academic Pressure": 6.0}),
    ]
    df = spark.createDataFrame(sample_data)
    city_degree_stats, demographic_stress, sleep_perf = aggregations(df)

    assert set(city_degree_stats.columns) == {"City", "Degree", "avg_depression", "count_students"}


    assert set(demographic_stress.columns) == {"age_group", "Gender", "avg_stress_index"}


    assert set(sleep_perf.columns) == {"sleep_category", "avg_cgpa", "avg_academic_score"}


def test_risk_analysis(spark):
    sample_data = [
        Row(stress_index_normalized=0.8, Sleep_Duration=7.0, Financial_Stress=4.0),
        Row(stress_index_normalized=0.6, Sleep_Duration=4.0, Financial_Stress=8.0),
        Row(stress_index_normalized=0.3, Sleep_Duration=8.0, Financial_Stress=3.0),
    ]
    df = spark.createDataFrame(sample_data)

    df_risk = risk_analysis(df)


    assert df_risk.count() == 2
    assert "risk_reason" in df_risk.columns


def test_parse_sleep_duration(spark):
    test_data = [
        ("Less than 5 hours", 4.0),
        ("5-6 hours", 5.5),
        ("More than 8 hours", 9.0),
        (None, None),
        ("invalid input", None),
        ("6-7 hours", 6.5),
        (" 7-8 hours ", 7.5),
    ]

    test_df = spark.createDataFrame(test_data, ["raw_input", "expected_output"])

    result_df = test_df.withColumn("parsed_output", parse_sleep_duration(col("raw_input")))

    results = result_df.select("expected_output", "parsed_output").collect()

    for row in results:
        assert row["expected_output"] == row["parsed_output"], \
            f"Expected {row['expected_output']}, but got {row['parsed_output']} for input '{row['raw_input']}'"

