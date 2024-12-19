from chispa.dataframe_comparer import *
from ..jobs.actor_scd_job import do_actor_scd_transformation
from collections import namedtuple

# Define the Actor namedtuple to represent the source data schema
# Fields:
# - actor: Name of the actor
# - actorid: Unique identifier for the actor
# - quality_class: A classification indicating the quality of the actor (e.g., "good" or "bad")
# - is_active: Boolean indicating if the actor is currently active
# - current_year: The current year for the data snapshot
Actor = namedtuple("Actor", "actor actorid quality_class is_active current_year")

# Define the ActorScd namedtuple to represent the expected output schema
# Fields:
# - actor: Name of the actor
# - actorid: Unique identifier for the actor
# - quality_class: A classification indicating the quality of the actor
# - is_active: Boolean indicating if the actor is currently active
# - start_year: The year when the actor's record starts
# - end_year: The year when the actor's record ends (may be null if still active)
# - current_year: The current year for the data snapshot
ActorScd = namedtuple("ActorScd", "actor actorid quality_class is_active start_year end_year current_year")


def test_scd_generation(spark):
    """
    Test function for the SCD (Slowly Changing Dimension) transformation logic.
    Validates the `do_actor_scd_transformation` function by comparing the actual output
    DataFrame with the expected DataFrame.

    Args:
        spark (SparkSession): The active Spark session used for creating DataFrames.
    """

    # Step 1: Define the source data
    # Represents the initial actor records to be transformed.
    source_data = [
        Actor("Adam Devine", "nm2796745", "good", True, 2017),  # Example actor 1
        Actor("George C. Scott", "nm0001715", "bad", True, 1999)  # Example actor 2
    ]

    # Step 2: Create a DataFrame from the source data
    # The schema is derived from the `Actor` namedtuple.
    source_df = spark.createDataFrame(source_data)

    # Step 3: Apply the transformation logic
    # Calls the `do_actor_scd_transformation` function to process the source DataFrame.
    actual_df = do_actor_scd_transformation(spark, source_df)

    # Step 4: Define the expected output data
    # Represents the expected result of the transformation.
    expected_data = [
        ActorScd("Adam Devine", "nm2796745", "good", True, 2017, 2017, 2021),  # Expected record for actor 1
        ActorScd("George C. Scott", "nm0001715", "bad", True, 1999, 1999, 2021)  # Expected record for actor 2
    ]

    # Step 5: Create a DataFrame from the expected data
    # The schema is derived from the `ActorScd` namedtuple.
    expected_df = spark.createDataFrame(expected_data)

    # Step 6: Assert equality between the actual and expected DataFrames
    # Uses chispa's `assert_df_equality` to validate that the transformation logic
    # produces the correct results.
    # The `ignore_nullable=True` parameter ensures that differences in schema nullability are ignored.
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
