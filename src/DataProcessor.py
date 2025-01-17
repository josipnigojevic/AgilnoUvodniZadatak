import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, avg, udf
from pyspark.sql.types import FloatType, IntegerType
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def create_spark_session(app_name="StudentMentalHealth"):
    """
    Creates and returns a SparkSession with recommended configs.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("Gender", StringType(), True),
    StructField("Age", DoubleType(), True),
    StructField("City", StringType(), True),
    StructField("Profession", StringType(), True),
    StructField("Academic Pressure", DoubleType(), True),
    StructField("Work Pressure", DoubleType(), True),
    StructField("CGPA", DoubleType(), True),
    StructField("Study Satisfaction", DoubleType(), True),
    StructField("Job Satisfaction", DoubleType(), True),
    StructField("Sleep Duration", StringType(), True),
    StructField("Dietary Habits", StringType(), True),
    StructField("Degree", StringType(), True),
    StructField("Have you ever had suicidal thoughts ?", StringType(), True),
    StructField("Work/Study Hours", DoubleType(), True),
    StructField("Financial Stress", DoubleType(), True),
    StructField("Family History of Mental Illness", StringType(), True),
    StructField("Depression", IntegerType(), True),
])

@udf(FloatType())
def parse_sleep_duration(raw: str) -> float:
    """
    Converts strings like "Less than 5 hours", "5-6 hours", 
    "More than 8 hours" into numeric float values.
    """
    if raw is None:
        return None
    raw = raw.strip().lower()
    if raw == "less than 5 hours":
        return 4.0
    elif raw == "more than 8 hours":
        return 9.0
    elif "-" in raw:
        raw = raw.replace(" hours", "")
        parts = raw.split("-")
        if len(parts) == 2:
            try:
                low = float(parts[0])
                high = float(parts[1])
                return (low + high) / 2.0
            except ValueError:
                return None
    return None

def load_data(spark, input_path: str):
    try:
        df = (spark.read
                .option("header", "true")
                .schema(schema)
                .csv(input_path))
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        sys.exit(1)


def clean_data(df):
    """
    Handle missing values, remove inconsistent or out-of-range values,
    convert Sleep Duration to numeric, etc.
    Returns cleaned DataFrame.
    """
    
    df = df.dropna(subset=["Sleep Duration", "Age"])

    
    df = df.withColumn("Sleep Duration", parse_sleep_duration(col("Sleep Duration")))
    
    df = df.filter(col("Sleep Duration") <= 24).filter(col("Sleep Duration") >= 0)
    df = df.filter(col("Age") > 0)

    return df


def report_data_quality(df):
    """
    Print out data quality metrics: null counts, value counts, basic statistics.
    In a real pipeline, these might be saved to a log or written to a table.
    """
    
    for c in df.columns:
        null_count = df.filter(col(c).isNull()).count()
        print(f"{c}: {null_count} nulls")

    
    numeric_cols = [f.name for f in df.schema.fields if f.dataType in [FloatType(), IntegerType()]]
    df.select(numeric_cols).describe().show()


def feature_engineering(df):
    """
    Create new columns:
      - stress_index (weighted average of certain columns)
      - sleep categories
      - age groups
      - normalized versions of numeric columns
      - dummy variables for categorical columns
    Returns a feature-engineered DataFrame.
    """

    df = df.withColumn(
        "stress_index",
        (col("Academic Pressure") + col("Work Pressure") + col("Financial Stress")) / lit(3.0)
    )

    df = df.withColumn(
        "sleep_category",
        when(col("Sleep Duration") < 6, lit("Low"))
        .when((col("Sleep Duration") >= 6) & (col("Sleep Duration") <= 8), lit("Normal"))
        .otherwise(lit("High"))
    )

    df = df.withColumn(
        "age_group",
        when((col("Age") >= 18) & (col("Age") <= 21), lit("18-21"))
        .when((col("Age") >= 22) & (col("Age") <= 25), lit("22-25"))
        .when((col("Age") >= 26) & (col("Age") <= 30), lit("26-30"))
        .otherwise(lit(">30"))
    )

    numeric_cols = ["CGPA", "Depression", "stress_index"]
    stats = df.select(
        *[F.min(c).alias(f"{c}_min") for c in numeric_cols],
        *[F.max(c).alias(f"{c}_max") for c in numeric_cols]
    ).collect()[0]


    for c in numeric_cols:
        min_val = stats[f"{c}_min"]
        max_val = stats[f"{c}_max"]
        if min_val != max_val:  
            df = df.withColumn(f"{c}_normalized", (col(c) - lit(min_val)) / (lit(max_val - min_val)))
        else:

            df = df.withColumn(f"{c}_normalized", lit(0.0))

    categories = [row[0] for row in df.select("Gender").distinct().collect()]
    for cat in categories:
        df = df.withColumn(
            f"Gender_{cat}",
            when(col("Gender") == cat, lit(1)).otherwise(lit(0))
        )

    return df


def save_parquet(df, output_path: str, partition_col: str = None):
    """
    Save a DataFrame to Parquet, with optional partitioning.
    Uses Snappy compression (from Spark config).
    """
    try:
        if partition_col:
            df.write.mode("overwrite").partitionBy(partition_col).parquet(output_path)
        else:
            df.write.mode("overwrite").parquet(output_path)
    except Exception as e:
        print(f"Error writing Parquet to {output_path}: {e}")


def distribution_analysis(df):
    """
    1) Depression scores by age group and profession
    2) CGPA stats by sleep category
    Return DataFrames for saving.
    """

    dep_by_demo = df.groupBy("age_group", "Profession") \
                    .agg(F.avg("Depression").alias("avg_depression"))


    cgpa_by_sleep = df.groupBy("sleep_category") \
                      .agg(F.avg("CGPA").alias("avg_cgpa"),
                           F.stddev("CGPA").alias("stddev_cgpa"))

    return dep_by_demo, cgpa_by_sleep


def correlation_analysis(df):
    """
    - Compute correlation matrix among numeric columns
    - Top 5 factors correlated with depression scores
    """
    numeric_cols = [f.name for f in df.schema.fields if f.dataType in [FloatType(), IntegerType()]]

    corr_rows = []
    for i in range(len(numeric_cols)):
        for j in range(i+1, len(numeric_cols)):
            col1 = numeric_cols[i]
            col2 = numeric_cols[j]
            corr_val = df.stat.corr(col1, col2)
            corr_rows.append((col1, col2, corr_val))

    correlation_df = df.sparkSession.createDataFrame(
        corr_rows, ["column1", "column2", "correlation"]
    )

    depression_corr = correlation_df \
        .filter((F.col("column1") == "Depression") | (F.col("column2") == "Depression")) \
        .withColumn("abs_corr", F.abs(F.col("correlation"))) \
        .orderBy(F.col("abs_corr").desc())

    top5_dep_corr = depression_corr.limit(5)

    return correlation_df, top5_dep_corr


def aggregations(df):
    """
    - Depression scores aggregated by city and degree
    - Stress index aggregated by age group and gender
    - Academic performance metrics by sleep category (reuse or expand)
    """

    city_degree_stats = df.groupBy("City", "Degree") \
                          .agg(F.avg("Depression").alias("avg_depression"),
                               F.count("*").alias("count_students"))

    demographic_stress = df.groupBy("age_group", "Gender") \
                           .agg(F.avg("stress_index").alias("avg_stress_index"))

    sleep_performance = df.groupBy("sleep_category") \
                          .agg(F.avg("CGPA").alias("avg_cgpa"),
                               F.avg("Academic Pressure").alias("avg_academic_score"))

    return city_degree_stats, demographic_stress, sleep_performance


def risk_analysis(df):
    """
    Identify high-risk students based on:
    - stress_index
    - sleep duration
    - academic/job satisfaction
    - financial stress
    Returns a DataFrame of flagged students
    """

    high_risk_df = df.filter(
        (col("stress_index_normalized") > 0.7) |
        (col("Sleep Duration") < 5) |
        (col("Financial Stress") > 7)  
    ).withColumn("risk_reason", lit("Stress or Poor Sleep or High Financial Stress"))

    return high_risk_df
