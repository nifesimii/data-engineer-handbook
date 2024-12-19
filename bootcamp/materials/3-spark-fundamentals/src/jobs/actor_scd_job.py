from pyspark.sql import SparkSession

# SQL query for Slowly Changing Dimensions (SCD) transformation
# This query processes actor records to group them into time-based streaks
# whenever there are changes in their `quality_class` or `is_active` status.
query = """
-- Step 1: Add previous year's data for comparison
WITH with_previous AS (
  SELECT 
    actor,
    actorid,
    quality_class,
    is_active,
    current_year,
    LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_quality_class, -- Previous year's quality_class
    LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_is_active          -- Previous year's is_active
  FROM actors
  WHERE current_year <= 2021 -- Restrict data to the range up to the current year (2021)
),

-- Step 2: Identify rows where a change occurred
with_indicators AS (
  SELECT *, 
    CASE 
      WHEN quality_class <> previous_quality_class THEN 1 -- Change in quality_class indicates a change
      WHEN is_active <> previous_is_active THEN 1         -- Change in is_active indicates a change
      ELSE 0                                              -- No change
    END AS change_indicator -- Add a column to indicate whether a change occurred
  FROM with_previous
),

-- Step 3: Assign a streak identifier based on changes
with_streaks AS (
  SELECT *,
    SUM(change_indicator) 
      OVER (PARTITION BY actor ORDER BY current_year) AS streak_identifier -- Create streaks by summing changes
  FROM with_indicators
)

-- Step 4: Aggregate records into start and end years for each streak
SELECT 
  actor,
  actorid,
  quality_class,
  is_active,
  MIN(current_year) AS start_year, -- Start of the streak
  MAX(current_year) AS end_year,   -- End of the streak
  CAST(2021 AS BIGINT) AS current_year -- Set the current year to 2021
FROM with_streaks
GROUP BY actor, actorid, streak_identifier, is_active, quality_class -- Group by streak and key attributes
ORDER BY actor, actorid; -- Order results by actor and actor ID
"""

# Function to perform the SCD transformation
def do_actor_scd_transformation(spark, dataframe):
    """
    Perform Slowly Changing Dimensions (SCD) transformation on the actor data.

    Args:
        spark (SparkSession): The active Spark session.
        dataframe (DataFrame): Input DataFrame containing actor data.

    Returns:
        DataFrame: Transformed DataFrame with start and end years for each streak.
    """
    # Register the input DataFrame as a temporary view for SQL query execution
    dataframe.createOrReplaceTempView("actors")
    
    # Execute the SQL query to perform the transformation and return the result as a DataFrame
    return spark.sql(query)

# Main function to set up the Spark application and execute the transformation
def main():
    """
    Main entry point for the Spark application.
    Sets up the Spark session, processes the actor data, and writes the output.
    """
    # Step 1: Initialize the Spark session,run Spark locally with a single worker and set the application name
    spark = SparkSession.builder \
      .master("local") \
      .appName("actors_scd") \
      .getOrCreate()

    # Step 2: Execute the SCD transformation
    # Fetch the input data from the "actors" Hive table
    output_df = do_actor_scd_transformation(
        spark, 
        spark.table("actors")  # Load the actor data table
    )

    # Step 3: Write the transformed data back to the output table
    # Overwrite the target Hive table "actors_scd" with the new SCD data
    output_df.write.mode("overwrite").insertInto("actors_scd")

# Run the main function when the script is executed
if __name__ == "__main__":
    main()
