import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import avg, col, count, desc, round, size, udf
from datetime import datetime

# Import your functions (assumed to be in the same file or accessible module)
from eligibility.rules import is_eligible_age, is_eligible_age_udf, has_recent_car_loan, has_recent_car_loan_udf

# Create SparkSession for testing (if needed)
@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("test") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "512m") \
        .getOrCreate()
    return spark

# Unit Test: Age Eligibility
def test_is_eligible_age():
    assert is_eligible_age(18) == True
    assert is_eligible_age(25) == True
    assert is_eligible_age(74) == True
    assert is_eligible_age(17) == False
    assert is_eligible_age(75) == False

def test_is_eligible_age_udf(spark):
    schema = StructType([StructField("age", IntegerType())])
    data = [(18,), (25,), (74,), (17,), (75,)]
    df = spark.createDataFrame(data, schema)

    df = df.withColumn("is_eligible", is_eligible_age_udf(col("age")))

    expected_results = [True, True, True, False, False]
    actual_results = [row.is_eligible for row in df.collect()]
    assert actual_results == expected_results

# Unit Test: Recent Car Loan
def test_has_recent_car_loan():
    today_str = datetime.now().strftime("%Y-%m-%d")  # Get today's date
    assert has_recent_car_loan("Car Loan", today_str) == True

    # Test 90 days ago
    from datetime import timedelta
    ninety_days_ago = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    assert has_recent_car_loan("Car Loan", ninety_days_ago) == True

    # Test 91 days ago and incorrect product name
    ninety_one_days_ago = (datetime.now() - timedelta(days=91)).strftime("%Y-%m-%d")
    assert has_recent_car_loan("Car Loan", ninety_one_days_ago) == False
    assert has_recent_car_loan("Personal Loan", today_str) == False

def test_has_recent_car_loan_udf(spark):
    schema = StructType([StructField("product_name", StringType()), StructField("event_date_str", StringType())])
    today_str = datetime.now().strftime("%Y-%m-%d")
    data = [("Car Loan", today_str), ("Personal Loan", today_str)]  # Add more test cases
    df = spark.createDataFrame(data, schema)

    df = df.withColumn("has_recent_loan", has_recent_car_loan_udf(col("product_name"), col("event_date_str")))

    # Check the results according to your test data
    expected_results = [True, False] # Adjust based on your test data
    actual_results = [row.has_recent_loan for row in df.collect()]
    assert actual_results == expected_results
