from pyspark.sql import SparkSession

# SQL query for cumulative transformation of user device activity
# This query generates an updated `device_activity_datelist` for each user
query = """
-- Creating a cumulative query to generate device_activity_datelist from events

WITH yesterday AS (
    -- Fetch data from the cumulative table for the previous day's snapshot
    SELECT * 
    FROM user_devices_cumulated
    WHERE date = DATE('2023-01-30')
),
today AS (
    -- Extract user activity from the source events for the current date (2023-01-31)
    SELECT 
        user_id,  
        browser_type,
        DATE(CAST(event_time AS TIMESTAMP)) AS date_active
    FROM source_events 
    WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
      AND user_id IS NOT NULL -- Exclude events with missing user IDs
      AND browser_type IS NOT NULL -- Exclude events with missing browser types
    GROUP BY user_id, browser_type, DATE(CAST(event_time AS TIMESTAMP)) -- Aggregate events by user, browser, and date
)

-- Combine the cumulative data (`yesterday`) and new events (`today`) to update device activity
SELECT
    COALESCE(t.user_id, y.user_id) AS user_id, -- Retain user_id from either today's or yesterday's data
    CASE 
        WHEN y.device_activity IS NULL THEN array(t.date_active) -- If no prior activity, start with today's date
        WHEN t.date_active IS NULL THEN y.device_activity -- If no new activity, retain previous activity
        ELSE array(t.date_active) || y.device_activity -- Combine today's and prior activity into an updated list
    END AS device_activity,
    COALESCE(t.browser_type, y.browser_type) AS browser_type, -- Retain the browser type from either source
    COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date_active -- Calculate the latest active date
FROM yesterday y
FULL OUTER JOIN today t 
    ON t.user_id = y.user_id AND t.browser_type = y.browser_type; -- Match records by user_id and browser_type
"""

# Function to perform the cumulative transformation
def do_user_devices_cumulated_cumm_transformation(spark, user_devices_cumulated, source_events):
    """
    Perform cumulative transformation to update device activity.

    Args:
        spark (SparkSession): The active Spark session.
        user_devices_cumulated (DataFrame): The existing cumulative table of user device activity.
        source_events (DataFrame): The new events data for the current date.

    Returns:
        DataFrame: The updated cumulative user device activity data.
    """
    # Register the input DataFrames as temporary views for SQL query execution
    user_devices_cumulated.createOrReplaceTempView("user_devices_cumulated")
    source_events.createOrReplaceTempView("source_events")
    
    # Execute the SQL query to perform the transformation and return the result as a DataFrame
    return spark.sql(query)

# Main function to set up the Spark application and execute the transformation
def main():
    """
    Main entry point for the Spark application.
    Sets up the Spark session, runs the transformation, and writes the output.
    """
    # Step 1: Initialize the Spark session, run Spark locally with a single worker and set the application name 
    spark = SparkSession.builder \
      .master("local") \
      .appName("actors_cumm") \
      .getOrCreate()

    # Step 2: Execute the transformation
    # Fetch input data from Hive tables (user_devices_cumulated and source_events)
    output_df = do_user_devices_cumulated_cumm_transformation(
        spark,
        spark.table("user_devices_cumulated"),  # Load the cumulative table
        spark.table("source_events")           # Load the new events table
    )

    # Step 3: Write the transformed data back to the output table
    # Overwrite the target Hive table "actors_cumm" with the new cumulative data
    output_df.write.mode("overwrite").insertInto("actors_cumm")

# Run the main function when the script is executed
if __name__ == "__main__":
    main()

