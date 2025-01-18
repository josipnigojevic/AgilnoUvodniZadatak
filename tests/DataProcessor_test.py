import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
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
               "Sleep Duration": "7-8 hours", "Dietary Habits": "Veg", "Degree": "Bachelor",
               "Have you ever had suicidal thoughts ?": "No",
               "Work/Study Hours": 5.0, "Financial Stress": 3.0,
               "Family History of Mental Illness": "No", "Depression": 2}),
        Row(id=2, Gender="Female", Age=24.0, City="CityB", Profession="Intern",
            **{"Academic Pressure": 4.0, "Work Pressure": 2.0, "CGPA": 3.5,
               "Study Satisfaction": 3.0, "Job Satisfaction": 4.0,
               "Sleep Duration": "5-6 hours", "Dietary Habits": "Non-Veg", "Degree": "Bachelor",
               "Have you ever had suicidal thoughts ?": "Yes",
               "Work/Study Hours": 6.0, "Financial Stress": 5.0,
               "Family History of Mental Illness": "Yes", "Depression": 4}),
    ]
    df_dirty = spark.createDataFrame(sample_data)

    df_clean = clean_data(df_dirty)

    assert df_clean.count() == 2
    assert dict(df_clean.dtypes)["Sleep Duration"] == "float"
    row = df_clean.filter(col("id") == 1).collect()[0]
    assert row["Sleep Duration"] == 7.5


def test_distribution_analysis(spark):

    sample_data = [
        {
            "id": 1, "Gender": "Male", "Age": 20.0, "City": "CityA", "Profession": "Student",
            "Academic Pressure": 5.0, "Work Pressure": 4.0, "CGPA": 3.5,
            "Study Satisfaction": 4.5, "Job Satisfaction": 3.0,
            "Sleep Duration": "7-8 hours", "Dietary Habits": "Veg", "Degree": "Bachelor",
            "Work/Study Hours": 6.0, "Financial Stress": 3.0,
            "Family History of Mental Illness": "No", "Depression": 2
        },
        {
            "id": 2, "Gender": "Female", "Age": 22.0, "City": "CityB", "Profession": "Intern",
            "Academic Pressure": 3.0, "Work Pressure": 3.0, "CGPA": 3.0,
            "Study Satisfaction": 3.0, "Job Satisfaction": 4.0,
            "Sleep Duration": "5-6 hours", "Dietary Habits": "Non-Veg", "Degree": "Bachelor",
            "Work/Study Hours": 7.0, "Financial Stress": 4.0,
            "Family History of Mental Illness": "Yes", "Depression": 4
        },
    ]


    df = spark.createDataFrame(sample_data, schema=schema)


    df_clean = clean_data(df)
    df_features = feature_engineering(df_clean)

    dep_by_demo, cgpa_by_sleep = distribution_analysis(df_features)


    assert set(dep_by_demo.columns) == {"age_group", "Profession", "avg_depression"}
    assert set(cgpa_by_sleep.columns) == {"sleep_category", "avg_cgpa", "stddev_cgpa"}


def test_correlation_analysis(spark):

    sample_data = [
        {
            "id": 1, "Gender": "Male", "Age": 20.0, "City": "CityA", "Profession": "Student",
            "Academic Pressure": 5.0, "Work Pressure": 4.0, "CGPA": 3.5,
            "Study Satisfaction": 4.5, "Job Satisfaction": 3.0,
            "Sleep Duration": "7-8 hours", "Dietary Habits": "Veg", "Degree": "Bachelor",
            "Work/Study Hours": 6.0, "Financial Stress": 3.0,
            "Family History of Mental Illness": "No", "Depression": 2
        },
        {
            "id": 2, "Gender": "Female", "Age": 22.0, "City": "CityB", "Profession": "Intern",
            "Academic Pressure": 3.0, "Work Pressure": 3.0, "CGPA": 3.0,
            "Study Satisfaction": 3.0, "Job Satisfaction": 4.0,
            "Sleep Duration": "5-6 hours", "Dietary Habits": "Non-Veg", "Degree": "Bachelor",
            "Work/Study Hours": 7.0, "Financial Stress": 4.0,
            "Family History of Mental Illness": "Yes", "Depression": 4
        },
    ]


    df = spark.createDataFrame(sample_data, schema=schema)

    df_clean = clean_data(df)
    df_features = feature_engineering(df_clean)

    corr_df, top5_df = correlation_analysis(df_features)


    assert set(corr_df.columns) == {"column1", "column2", "correlation"}
    assert corr_df.count() > 0


def test_aggregations(spark):

    sample_data = [
        {
            "id": 1, "Gender": "Male", "Age": 20.0, "City": "CityA", "Profession": "Student",
            "Academic Pressure": 5.0, "Work Pressure": 4.0, "CGPA": 3.5,
            "Study Satisfaction": 4.5, "Job Satisfaction": 3.0,
            "Sleep Duration": "7-8 hours", "Dietary Habits": "Veg", "Degree": "Bachelor",
            "Work/Study Hours": 6.0, "Financial Stress": 3.0,
            "Family History of Mental Illness": "No", "Depression": 2
        },
        {
            "id": 2, "Gender": "Female", "Age": 22.0, "City": "CityB", "Profession": "Intern",
            "Academic Pressure": 3.0, "Work Pressure": 3.0, "CGPA": 3.0,
            "Study Satisfaction": 3.0, "Job Satisfaction": 4.0,
            "Sleep Duration": "5-6 hours", "Dietary Habits": "Non-Veg", "Degree": "Bachelor",
            "Work/Study Hours": 7.0, "Financial Stress": 4.0,
            "Family History of Mental Illness": "Yes", "Depression": 4
        },
    ]


    df = spark.createDataFrame(sample_data, schema=schema)


    df_clean = clean_data(df)
    df_features = feature_engineering(df_clean)

    city_degree_stats, demographic_stress, sleep_perf = aggregations(df_features)


    assert set(city_degree_stats.columns) == {"City", "Degree", "avg_depression", "count_students"}
    assert set(demographic_stress.columns) == {"age_group", "Gender", "avg_stress_index"}
    assert set(sleep_perf.columns) == {"sleep_category", "avg_cgpa", "avg_academic_score"}


def test_risk_analysis(spark):

    sample_data = [
        {
            "id": 1, "Gender": "Male", "Age": 20.0, "City": "CityA", "Profession": "Student",
            "Academic Pressure": 5.0, "Work Pressure": 4.0, "CGPA": 3.5,
            "Study Satisfaction": 4.5, "Job Satisfaction": 3.0,
            "Sleep Duration": "7-8 hours", "Dietary Habits": "Veg", "Degree": "Bachelor",
            "Work/Study Hours": 6.0, "Financial Stress": 8.0,
            "Family History of Mental Illness": "No", "Depression": 2
        },
        {
            "id": 2, "Gender": "Female", "Age": 22.0, "City": "CityB", "Profession": "Intern",
            "Academic Pressure": 3.0, "Work Pressure": 3.0, "CGPA": 3.0,
            "Study Satisfaction": 3.0, "Job Satisfaction": 4.0,
            "Sleep Duration": "5-6 hours", "Dietary Habits": "Non-Veg", "Degree": "Bachelor",
            "Work/Study Hours": 7.0, "Financial Stress": 10.0,
            "Family History of Mental Illness": "Yes", "Depression": 4
        },
    ]


    df = spark.createDataFrame(sample_data, schema=schema)


    df_clean = clean_data(df)
    df_features = feature_engineering(df_clean)

    df_risk = risk_analysis(df_features)


    assert df_risk.count() > 0
    assert "risk_reason" in df_risk.columns


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

