import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session


def create_session_events_sink_postgres(t_env):
    """
    Create a PostgreSQL sink table for storing session events.
    
    :param t_env: StreamTableEnvironment instance.
    :return: Name of the created table.
    """
    table_name = 'session_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            session_start TIMESTAMP(3),  -- Start time of the session
            session_end TIMESTAMP(3),    -- End time of the session
            ip VARCHAR,                  -- IP address of the client
            host VARCHAR,                -- Host name or domain
            num_events BIGINT            -- Total number of events in the session
        ) WITH (
            'connector' = 'jdbc',        -- Connector type: JDBC
            'url' = '{os.environ.get("POSTGRES_URL")}',          -- PostgreSQL connection URL
            'table-name' = '{table_name}',                       -- Target table name in PostgreSQL
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}', -- PostgreSQL username
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}', -- PostgreSQL password
            'driver' = 'org.postgresql.Driver'                   -- JDBC driver for PostgreSQL
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_processed_events_source_kafka(t_env):
    """
    Create a Kafka source table for reading processed events.

    :param t_env: StreamTableEnvironment instance.
    :return: Name of the created table.
    """
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "process_events_kafka"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"  # Timestamp format for event time
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,                   -- IP address of the client
            event_time VARCHAR,           -- Raw event timestamp as a string
            referrer VARCHAR,             -- Referrer URL
            host VARCHAR,                 -- Host name or domain
            user_agent VARCHAR,           -- Client's user agent
            url VARCHAR,                  -- URL of the event
            geodata VARCHAR,              -- Geographical data
            window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'), -- Converted event timestamp
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND -- Watermark for event time
        ) WITH (
            'connector' = 'kafka',                             -- Connector type: Kafka
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}', -- Kafka broker URL
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',       -- Kafka topic name
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}', -- Kafka consumer group ID
            'properties.security.protocol' = 'SASL_SSL',      -- Security protocol
            'properties.sasl.mechanism' = 'PLAIN',            -- Authentication mechanism
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',            -- Read from the latest offset
            'properties.auto.offset.reset' = 'latest',        -- Reset behavior if no offset is available
            'format' = 'json'                                 -- Message format: JSON
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_session():
    """
    Define the Flink job to process and aggregate session events from Kafka,
    and write them to a PostgreSQL sink.
    """
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10)  # Enable checkpointing every 10 ms
    env.set_parallelism(3)       # Set the parallelism level

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka source table
        source_table = create_processed_events_source_kafka(t_env)

        # Create PostgreSQL sink table for session aggregation
        session_aggregated_table = create_session_events_sink_postgres(t_env)

        # Process events and compute session-level aggregates
        t_env.from_path(source_table)\
            .window(
                Session.with_gap(lit(5).minutes).on(col("window_timestamp")).alias("w")  # Session window with a 5-minute gap
            ).group_by(
                col("w"),  # Group by session window
                col("ip"),  # Group by IP address
                col("host")  # Group by host name
            ).select(
                col("w").start.alias("session_start"),  # Start time of the session
                col("w").end.alias("session_end"),      # End time of the session
                col("ip"),                              # IP address
                col("host"),                            # Host name
                col("host").count.alias("num_events")   # Number of events in the session
            ).execute_insert(session_aggregated_table)  # Write to PostgreSQL sink

    except Exception as e:
        # Log any errors that occur during processing
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_session()
