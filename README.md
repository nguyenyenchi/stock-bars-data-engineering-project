# 🧠 ETL Pipeline for Historical Stock Bars Analysis 
End-to-End Data Engineering Project — Extract, Transform, Load, and Deploy on AWS
## 📋 Project Overview

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline that extracts historical daily stock bars (open, high, low, close, volume) from the Alpaca Markets API, performs analytical transformations, stores the data in a PostgreSQL database hosted on Amazon Relational Database Service (RDS) and deploys the automated workflow with a weekly run schedule to AWS ECS (Elastic Container Service).

The pipeline runs initially as a full extract and load, and subsequently performs incremental extract and load based on the latest timestamp recorded in the database.
Processed and enriched data is stored in a separate table named stock_bars_analysis, which can be used for downstream analytics and dashboarding.

## 🧩 Key Features

| Key features | Description |
| ----------- | ----------- |
| Data source | Live dataset from the [Historical bars on Alpaca Markets API](https://docs.alpaca.markets/reference/stockbars) and we get daily stock bar data for top 10 technology companies. |
| Extract method | Full extract (initial load) and incremental extract (subsequent runs using latest timestamp from last run).|
|Load Method |	Full load (initially overwriting data) followed by incremental load (upserting records using latest timestamp from last run). |
| Data transformation | Rename column, get date from timestamp, windown functions to calculate previous close price, daily returns, 5-day moving average and standard deviation.  All transformed data is stored in a separate table named stock_bars_analysis for further analysis.|
|Unit testings| Unit testing for connectors (Alpaca Markets API and PostgreSQL) modules |
| Metadata logging | Pipeline metadata logged in a dedicated table |
| Containerization |	Built and packaged as a Docker image, stored in AWS Elastic Container Registry (ECR). |
| Orchestration	| Deployed as a scheduled Elastic Container Service (ECS) task running weekly. |
| Storage	| PostgreSQL database hosted on AWS RDS. |
|Configuration |	Environment variables managed securely via .env file stored in AWS S3.|


## 🚀 Run the Docker locally

Dockerize the ETL Pipeline by following these steps:

1. Clone this repository

1. Build the docker image using this command:

   ```
   docker build . -t stock_bars:1.0
   ```

2. Configure your `.env` file (update using your own example):

   ```
    APCA-API-KEY-ID=xxx
    APCA-API-SECRET-KEY=xxx
    DB_USERNAME=postgres
    DB_PASSWORD=postgres
    SERVER_NAME=host.docker.internal
    DATABASE_NAME=my_database 
   ```

3. Run your docker image using this command:

   Run without volume:

   ```
   docker run --env-file .env stock_bars:1.0
   ```

   Run with volume:

   ```
   docker run --env-file .env -v stock_bars:/app/ stock_bars:1.0
   ```

## Deploy docker container to Amazon Web Services

### Image created in Elastic Container Registry (ECR)
![alt text](/instruction/images/image.png)

### IAM role for ECS to access S3 files

![alt text](/instruction/images/image-5.png)

### .env file uploaded to a private S3 bucket
![alt text](/instruction/images/image-4.png)

### RDS for data storage
![alt text](/instruction/images/image-6.png)

Example of data in stock_bars_analysis table

![alt text](/instruction/images/image-7.png)

### Scheduled task in Elastic Container Service (ECS)
A my-etl-schedule is created to run every 7 days for my-etl-ecs cluster
![alt text](/instruction/images/image-2.png)

### Logs of successful task run
![alt text](/instruction/images/image-3.png)
