from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf
from pyspark.sql.functions import col, when

# Age eligibility
def is_eligible_age(age):
    """
    Checks if an individual's age falls within the eligible range (18 to 74 years old, inclusive).

    Args:
    age: The age of the individual.

    Returns:
    True if the age is within the eligible range, False otherwise.
    """
    return 18 <= age <= 74

is_eligible_age_udf = udf(is_eligible_age, BooleanType())

# Has recent car loan 
def has_recent_car_loan(product_name, event_date_str):
    """
    Checks if a car loan was taken out within the last 90 days.

    Args:
        product_name: The name of the product (e.g., "Car Loan").
        event_date_str: The event date as a string (e.g., "2024-03-17").

    Returns:
        True if a car loan was taken within the last 90 days, False otherwise.
    """
    from datetime import datetime
    if product_name != "Car Loan":
        return False

    event_date = datetime.strptime(event_date_str, "%Y-%m-%d")
    today = datetime.now()
    days_since_loan = (today - event_date).days
    return days_since_loan <= 90



has_recent_car_loan_udf = udf(has_recent_car_loan, BooleanType())
