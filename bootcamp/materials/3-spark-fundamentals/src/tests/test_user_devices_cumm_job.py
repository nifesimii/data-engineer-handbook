from chispa.dataframe_comparer import *
from ..jobs.user_devices_cumm_job import do_user_devices_cumulated_cumm_transformation


def test_scd_generation(spark):
    """
    Test function for the SCD (Slowly Changing Dimension) transformation logic.
    This test validates that the `do_user_devices_cumulated_cumm_transformation` function processes the input data correctly
    and produces the expected output.

    Args:
        spark (SparkSession): The active Spark session for creating and testing DataFrames.
    """

    # Step 1: Create the input DataFrame `source_events`
    # Represents new user activity data for a specific day (e.g., 2023-01-31).
    # Columns:
    # - user_id: Unique identifier for the user
    # - browser_type: Type of browser used by the user
    # - event_time: Timestamp of the event
    source_events = spark.createDataFrame([
        (4487362073953144000, "Googlebot", "2023-01-31")  # Sample user event
    ], ["user_id", "browser_type", "event_time"])

    # Step 2: Create the input DataFrame `source_user_cumulative`
    # Represents the cumulative device activity data up to the current date.
    # Columns:
    # - user_id: Unique identifier for the user
    # - device_activity: List of dates when the user was active
    # - browser_type: Type of browser associated with the user
    # - date: The current cumulative date snapshot
    source_user_cumulative = spark.createDataFrame([
        (
            4487362073953144000,
            ["2023-01-31", "2023-01-30", "2023-01-29", "2023-01-28", "2023-01-27", "2023-01-26"],
            "Googlebot",
            "2023-01-31"
        )  # Historical device activity for the user
    ], ["user_id", "device_activity", "browser_type", "date"])

    # Step 3: Call the transformation function
    # This function applies the business logic to combine `source_user_cumulative` and `source_events`
    # to produce an updated cumulative record.
    actual_df = do_user_devices_cumulated_cumm_transformation(
        spark, 
        source_user_cumulative, 
        source_events
    )

    # Step 4: Define the expected output DataFrame `expected_df`
    # Represents the expected state of the cumulative device activity after applying the transformation.
    # Columns:
    # - user_id: Unique identifier for the user
    # - device_activity: Updated list of dates when the user was active
    # - browser_type: Type of browser associated with the user
    # - date_active: Date when the user was last active
    expected_df = spark.createDataFrame([
        (
            4487362073953144000,
            ["2023-01-31"],  # Only the current event date should remain in the updated list
            "Googlebot",
            "2023-01-31"  # The last active date matches the current event date
        )
    ], ["user_id", "device_activity", "browser_type", "date_active"])

    # Step 5: Assert that the actual DataFrame matches the expected DataFrame
    # Uses chispa's `assert_df_equality` to perform a comparison between the actual and expected DataFrames.
    # The `ignore_nullable=True` parameter ensures the schema's nullability differences are ignored during comparison.
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
