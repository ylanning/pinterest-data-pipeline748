# pinterest-data-pipeline748


## Project Overview

Every day, Pinterest processes an immense volume of data to enhance user experiences by leveraging advanced machine learning engineering systems. These systems handle billions of daily interactions, including image uploads and clicks, necessitating daily data processing to inform strategic decisions. The goal of this project is to create a system that emulates Pinterest's data analysis infrastructure, capable of analyzing both historical and real-time data generated through user posts.

## Technology Stack

The project utilizes a robust set of technologies to achieve its objectives:

**Apache Kafka**<br>
An event streaming platform facilitating real-time data capture and processing from diverse sources.

**Amazon MSK (Managed Streaming for Apache Kafka)**<br>
A fully managed AWS service for building applications using Apache Kafka for streaming data processing.

**AWS MSK Connect**<br>
A feature of Amazon MSK that simplifies streaming data to and from Apache Kafka clusters through managed connectors.

**Kafka REST Proxy**<br>
Offers a RESTful interface for interacting with Apache Kafka clusters, simplifying message production, consumption, and administrative tasks.

**AWS API Gateway**<br>
A fully managed service for creating, publishing, maintaining, monitoring, and securing APIs at scale.

**Apache Spark**<br>
A multi-language engine for executing data engineering, data science, and machine learning tasks on single-node machines or clusters.

**PySpark**<br>
Python API for Apache Spark, enabling real-time, large-scale data processing in a distributed environment using Python.

**Databricks**<br>
A unified, open analytics platform for building, deploying, sharing, and maintaining enterprise-grade data, analytics, and AI solutions at scale.

**Managed Workflows for Apache Airflow (MWAA)**<br>
An AWS service allowing the use of Apache Airflow and Python to create workflows without managing underlying infrastructure.

**AWS Kinesis**<br>
A managed service for processing and analyzing streaming data.<br>

## Project Milestones/Tasks
The project was divided into milestones as outlined below.

**Milestone 3 Batch Processing: Configure the EC2 Kafka client**
- Task 1 : Create a .pem key file locally
- Task 2 : Connect to the EC2 instance
- Task 3 : Set up Kafka on the EC2 instance
- Task 4 : Create Kafka topics
  
**Milestone 4 Batch Processing: Connect an MSK cluster to a S3 bucket**
- Task 1 : Create a custom plugin with MSK Connect
- Task 2 : Create a connector with MSK Connect
  
**Milestone 5 Batch Processing: Configuring an API in API Gateway**
- Task 1 : Build a Kafka REST proxy integration method for the API
- Task 2 : Setup the Kafka REST proxy on the EC2 client
- Task 3 : Send Data to the API

Working Files:
- user_posting_emulation.py

**Milestone 6 Batch Processing: Databricks**
- Task 1: Setup own Databricks account
- Task 2: Mount a S3 bucket to Databricks

Working Files:
- notebooks/batch_mount_S3_buckets.ipynb
  
**Milestone 7 Batch Processing: Spark on Databricks**
- Task 1, 2 and 3: Clean the Dataframes
- Task 4,5,6,7,8,9,10,11 : SQL queries
- 
Working Files:
- notebooks/0a2f66c3e41f_df_pin.ipynb
- notebooks/0a2f66c3e41f_df_geo.ipynb
- notebooks/0a2f66c3e41f_df_user.ipynb
- notebooks/sql_data_queries.ipynb

**Milestone 8 Batch Processing: AWS MWAA**
- Task 1: Create and upload a DAG to a MWAA environment
- Task 2: Trigger a DAG that runs a Databricks Notebook
  
Working Files:
- 0a2f66c3e41f_dag.py

**Milestone 9 Stream Processing: AWS Kinesis**
- Task 1: Create data streams using Kinesis Data Streams
- Task 2: Configure an API with Kinesis proxy integration
- Task 3: Send data to the Kinesis streams
- Task 4: Read data from Kinesis streams Databricks
- Task 5: Transform Kinesis streams in Databricks
- Task 6: Write the streaming data to Delta Tables

Working Files:
- user_posting_emulation_streaming.py
- notebooks/kinesis Data Streams.ipynb
