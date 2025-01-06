import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session


def create_session_events_sink_postgres(t_env: StreamTableEnvironment) -> str:
    """
    Creates a PostgreSQL sink table to store sessionized event data.

    This table captures session start and end times, IP addresses, host information,
    and the number of events within each session.

    Args:
        t_env: The StreamTableEnvironment used to execute the DDL statement.

    Returns:
        The name of the created sink table.
    """

    table_name = 'session_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            ip VARCHAR,
            host VARCHAR,
            num_events BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_processed_events_source_kafka(t_env: StreamTableEnvironment) -> str:
    """
    Creates a Kafka source table to read processed web event data.

    This function retrieves event data from a Kafka topic containing web traffic information.
    The data includes columns like IP address, event timestamp, referrer, host, user agent, and URL.

    Args:
        t_env: The StreamTableEnvironment used to define the source table.

    Returns:
        The name of the created source table.
    """

    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "process_events_kafka"
    pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            user_agent VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_session():
    """
    Main function to process web event data and write sessionized events to PostgreSQL.

    This function performs the following steps:
        1. Sets up the Flink execution environment with checkpointing and parallelism.
        2. Creates a StreamTableEnvironment for Apache Flink.
        3. Defines the Kafka source table to read processed web event data.
        4. Defines the PostgreSQL sink table to store sessionized events.
        5. Performs session windowing on the event stream.
        6. Aggregates events within each session window.
        7. Writes the sessionized data to the PostgreSQL sink table.
        8. Handles potential exceptions during the process.
    """
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10)  # Enable checkpointing for fault tolerance
    env.set_parallelism(3)         # Set the degree of parallelism

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka source table
        source_table = create_processed_events_source_kafka(t_env)

        # Create PostgreSQL sink table
        session_aggregated_table = create_session_events_sink_postgres(t_env)

        # Perform session windowing and aggregation
        t_env.from_path(source_table) \
            .window(
                Session.with_gap(lit(5).minutes).on(col("window_timestamp")).alias("w") 
            ) \
            .group_by(
                col("w"),
                col("ip"),
                col("host")
            ) \
            .select(
                col("w").start.alias("session_start"),
                col("w").end.alias("session_end"),
                col("ip"),
                col("host"),
                col("host").count.alias("num_events") 
            ) \
            .execute_insert(session_aggregated_table)

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_session()