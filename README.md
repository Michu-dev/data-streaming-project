# Data streaming project (netflix-prize-data) - short overview

Project made for Data Streaming Processing in Big Data, written in Spark Structured Streaming, ETL tasks orchestrated by Apache Airflow, deployed in GCP. Folder _project_description_ contains details about the project requirements. To sum up project require files stucture description and accurate unit test for ETL tasks.

**etl_jobs.py** - contains data processing logic (Apache Spark)

**orchestrate.py** - dataflow (Apache Airflow)

**sink_commands.sh, kafka_producer_setup.sh, ddl.sql** - scripts needed to setup environment in GCP

**KafkaProducer** - folder with Producer in Apache Kafka to ingest data streams

**tests** - folder for unit testing in pytest
