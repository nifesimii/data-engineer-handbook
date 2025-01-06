# Apache Flink Training
Week 4 Apache Flink Streaming Pipelines

## :pushpin: Getting started 

### :whale: Installations

To run this repo, the following components will need to be installed:

1. [Docker](https://docs.docker.com/get-docker/) (required)
2. [Docker compose](https://docs.docker.com/compose/install/#installation-scenarios) (required)
3. Make (recommended) -- see below
    - On most Linux distributions and macOS, `make` is typically pre-installed by default. To check if `make` is installed on your system, you can run the `make --version` command in your terminal or command prompt. If it's installed, it will display the version information. 
    - Otherwise, you can try following the instructions below, or you can just copy+paste the commands from the `Makefile` into your terminal or command prompt and run manually.

        ```bash
        # On Ubuntu or Debian:
        sudo apt-get update
        sudo apt-get install build-essential

        # On CentOS or Fedora:
        sudo dnf install make

        # On macOS:
        xcode-select --install

        # On windows:
        choco install make # uses Chocolatey, https://chocolatey.org/install
        ```

### :computer: Local setup

Clone/fork the repo and navigate to the root directory on your local computer.

```bash
git clone https://github.com/DataExpert-io/data-engineer-handbook.git
cd bootcamp/materials/4-apache-flink-training
```

### :dizzy: Configure credentials

1. Copy `example.env` to `flink-env.env`.

    ```bash
    cp example.env flink-env.env
    ```

2. Use `vim` or your favorite text editor to update `KAFKA_PASSWORD`, `KAFKA_GROUP`, `KAFKA_TOPIC`, and `KAFKA_URL` with the credentials in the `flink-env.env`

    ```bash
    vim flink-env.env
    ```
    
    ```bash
   KAFKA_WEB_TRAFFIC_SECRET="<GET FROM bootcamp.techcreator.io or email>"
   KAFKA_WEB_TRAFFIC_KEY="<GET FROM bootcamp.techcreator.io or email>
   IP_CODING_KEY="MAKE AN ACCOUNT AT https://www.ip2location.io/ TO GET KEY"
   
   KAFKA_GROUP=web-events
   KAFKA_TOPIC=bootcamp-events-prod
   KAFKA_URL=pkc-rgm37.us-west-2.aws.confluent.cloud:9092
   
   FLINK_VERSION=1.16.0
   PYTHON_VERSION=3.7.9
   
   POSTGRES_URL="jdbc:postgresql://host.docker.internal:5432/postgres"
   JDBC_BASE_URL="jdbc:postgresql://host.docker.internal:5432"
   POSTGRES_USER=postgres
   POSTGRES_PASSWORD=postgres
   POSTGRES_DB=postgres
    ```

    **:exclamation: Please do *not* push or share the environment file outside the bootcamp as it contains the credentials to cloud Kafka resources that could be compromised. :exclamation:**

    Other notes ~

    &rarr; _You can safely ignore the rest of the credentials in the `flink-env.env` file in Discord since the repo has since been updated and everything else you need is conveniently included in the `example.env`._

    &rarr; _You might also need to modify the configurations for the containerized postgreSQL instance such as `POSTGRES_USER` and `POSTGRES_PASSWORD`. Otherwise, you can leave the default username and password as `postgres`._


## :boom: Running the pipeline

1. Build the Docker image and deploy the services in the `docker-compose.yml` file, including the PostgreSQL database and Flink cluster. This will (should) also create the sink table, `processed_events`, where Flink will write the Kafka messages to.

    ```bash
    make up

    #// if you dont have make, you can run:
    # docker compose --env-file flink-env.env up --build --remove-orphans  -d
    ```

    **:star: Wait until the Flink UI is running at [http://localhost:8081/](http://localhost:8081/) before proceeding to the next step.** _Note the first time you build the Docker image it can take anywhere from 5 to 30 minutes. Future builds should only take a few second, assuming you haven't deleted the image since._

    :information_source: After the image is built, Docker will automatically start up the job manager and task manager services. This will take a minute or so. Check the container logs in Docker desktop and when you see the line below, you know you're good to move onto the next step.

    ```
    taskmanager Successful registration at resource manager akka.tcp://flink@jobmanager:6123/user/rpc/resourcemanager_* under registration id <id_number>
    ```
   2. Make sure to run `sql/init.sql` on the postgres database from Week 1 and 2 to have the `processed_events` table appear
3. Now that the Flink cluster is up and running, it's time to finally run the PyFlink job! :smile:

    ```bash
    make job

    #// if you dont have make, you can run:
    # docker-compose exec jobmanager ./bin/flink run -py /opt/src/job/start_job.py -d
    ```

    After about a minute, you should see a prompt that the job's been submitted (e.g., `Job has been submitted with JobID <job_id_number>`). Now go back to the [Flink UI](http://localhost:8081/#/job/running) to see the job running! :tada:

4. Trigger an event from the Kafka source by visiting [https://bootcamp.techcreator.io/](https://bootcamp.techcreator.io/) and then query the `processed_events` table in your postgreSQL database to confirm the data/events were added.

    ```bash
    make psql
    # or see `Makefile` to execute the command manually in your terminal or command prompt

    # expected output:
    docker exec -it eczachly-flink-postgres psql -U postgres -d postgres
    psql (15.3 (Debian 15.3-1.pgdg110+1))
    Type "help" for help.

    postgres=# SELECT COUNT(*) FROM processed_events;
    count 
    -------
    739
    (1 row)
    ```

## Assignment Solution
5.  Assuming you have run the commands above, the flink cluster and  and postgress databases should be running  so next we run the make command for pyflink code

    ```bash
    make session_job:

    #// if you dont have make, you can run:
    # docker compose exec jobmanager ./bin/flink run -py /opt/src/job/session_job.py --pyFiles /opt/src -d
    ```

    After about a minute, you should see a prompt that the job's been submitted (e.g., `Job has been submitted with JobID <job_id_number>`). Now go back to the [Flink UI](http://localhost:8081/#/job/running) to see the job running! :tada:

    Next we create the session table ddl in postgresql  with the command belows 

    ```bash
    psql (15.3 (Debian 15.3-1.pgdg110+1))
    Type "help" for help.

    postgres=# CREATE TABLE session_events(
        session_start TIMESTAMP(3),
        session_end TIMESTAMP(3),  
        ip VARCHAR,       
        host VARCHAR,
        num_events BIGINT
        );
    ---------
    CREATE TABLE
    Query returned successfully in 75 msec.
    ```

    Then we finaly run the script to get the avarage number of web event of a user based on the host said user is visiting 
    ```bash
        psql (15.3 (Debian 15.3-1.pgdg110+1))
    Type "help" for help.

    postgres=# select  ip as user_ip,host,CAST(AVG(num_events) AS BIGINT) AS Average_num_events from session_events
    where host = 'www.dataexpert.io'  and ip = '74.96.167.138' GROUP BY ip , host
    -------------------------------
    user_ip host Average_num_events
    -------------------------------
    74.96.167.138  www.dataexpert.io    6 
    (1 row)
    ```

    From the above query result, its clear the user with ip number `74.96.167.138` has visited www.dataexpert.io  6 times since the flink job started.  it should be noted that results can vary depending on the host and user ip used in the query as can be seen in the examples below based on the host name used in host parameter

    ```
    -----------------------------------------------
    user_ip host Average_num_events
    -----------------------------------------------
    74.96.167.138  zachwilson.techcreator.io    3
    (1 row)
    ```

    ```
    -----------------------------------------------
    user_ip host Average_num_events
    -----------------------------------------------
    74.96.167.138  zachwilson.tech    5
    (1 row)
    ```

     ```
    -----------------------------------------------
    user_ip host Average_num_events
    -----------------------------------------------
    74.96.167.138  lulu.techcreator.io   9
    (1 row)
    ```

    The results above show the output  the average session events for a specific IP address with the -different hosts - zachwilson.techcreator.io, zachwilson.tech, and lulu.techcreator.io


6. When you're done, you can stop and/or clean up the Docker resources by running the commands below.

    ```bash
    make stop # to stop running services in docker compose
    make down # to stop and remove docker compose services
    make clean # to remove the docker container and dangling images
    ```

    :grey_exclamation: Note the `/var/lib/postgresql/data` directory inside the PostgreSQL container is mounted to the `./postgres-data` directory on your local machine. This means the data will persist across container restarts or removals, so even if you stop/remove the container, you won't lose any data written within the container.

------

:information_source: To see all the make commands that're available and what they do, run:

```bash
make help
```

As of the time of writing this, the available commands are:

```bash

Usage:
  make <target>

Targets:
  help                 Show help with `make help`
  db-init              Builds and runs the PostgreSQL database service
  build                Builds the Flink base image with pyFlink and connectors installed
  up                   Builds the base Docker image and starts Flink cluster
  down                 Shuts down the Flink cluster
  job                  Submit the Flink job
  stop                 Stops all services in Docker compose
  start                Starts all services in Docker compose
  clean                Stops and removes the Docker container as well as images with tag `<none>`
  psql                 Runs psql to query containerized postgreSQL database in CLI
  postgres-die-mac     Removes mounted postgres data dir on local machine (mac users) and in Docker
  postgres-die-pc      Removes mounted postgres data dir on local machine (PC users) and in Docker
```


### Its important to note certain performance limitations can be encountered during the development of this job. Here are seven of such limitations and how to addrees them 

Limiatation One:
Backpressure Handling; High message throughput in Kafka can overwhelm the  PostgreSQL 
Solution One :
Instance Implement checkpointing and proper parallelism in the Flink job to avoid backpressure.
Tune Kafka consumer settings (e.g., fetch size, poll interval) to balance performance and resource usage.
Parallelism Configuration. Ensure the Kafka source and PostgreSQL sink have adequate parallelism to process data efficiently


Limitation Two 
Data Serialization: Misaligned types can cause deserialization or JDBC errors, slowing down the pipeline.
Solution Two 
Ensure data types between Kafka, Flink, and PostgreSQL are compatible.


Limitation Three
Late Events : Late events are events that arrive after the watermark has passed the end of the window
Solution Three
Properly configure watermarks in the Kafka source to handle late-arriving data while avoiding excessive memory consumption.

Limitations Four
Database Write Bottleneck: Use connection pooling libraries or increase PostgreSQL’s max_connections setting.
Solution Four
PostgreSQL may struggle with high-frequency inserts, leading to contention or connection pool exhaustion. 

Limitation Five
Fault Tolerance: If the Flink job fails, partial writes to PostgreSQL can occur.
Solution Five
Implement idempotent writes or a deduplication mechanism in PostgreSQL to avoid duplicate data.

Limitation Six
Schema Evolution : Changes in the Kafka message schema or PostgreSQL table schema can break the pipeline.
Solution Six:
Employ schema registry tools (e.g., Apache Avro or Confluent Schema Registry) to manage schema changes.

Limitation Seven
Data Skew : Uneven distribution of data (e.g., hotspot IPs) can lead to imbalanced processing across Flink operators.
Solution Seven
Implement partitioning strategies to reduce skew.
