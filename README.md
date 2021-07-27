# Data pipeline for Corporate Ecommerce Company

## Data Engineering Capstone Project

### Project Summary

This project objective aims to build a data pipeline for a corporate eCommerce company to move the data from the S3 bucket to stage tables and generate a star schema model ready for analysts to create dashboards and generate insights to raise sales.

The project follows the following steps:

    * Step 1: Scope the Project and Gather Data
    * Step 2: Explore and Assess the Data
    * Step 3: Define the Data Model
    * Step 4: Run ETL to Model the Data
    * Step 5: Complete Project Write Up

## Step 1: Scope the Project and Gather Data

### Scope

In this project, our goal is to build a data pipeline for a corporate eCommerce company to build a star schema data model so that analysis can be performed based on that data.

### Tools

   **Airflow**: Workflow manager with programmatic access
   
   **Python**: Programming language where Airflow is based

### End-Use Cases

There are some business goals of building this pipeline:

    1. The analytics team can use the star schema to get actionable insights from data
    2. The eCommerce business team can build dashboards to report KPIs to senior management
    3. It helps to track the evolution of sales and what triggers that evolution in time
    4. It can be the source of truth for eCommerce sales
 
### Describe and Gather Data

### Data Sources

The data would come from 3 different sources:

    1. The CRM platform: It sends daily the file corp_customer.csv in CSV format, so it can have detailed data from current customers.
    2. The SAP platform: It sends daily the file corp_products.json in JSON format, so it can have detailed data from current products.
    3. Google Analytics: It sends events in year-month.csv with CSV format, so it can know what events are performed by each customer. It has more than one million rows.
    
    
## Step 2: Explore and Assess the Data

### Explore the Data

There were found a few quality issues exploring these three datasets:

    * Products Dataset: Fields price, rating y view_count are empty
    * Event dataset: Most of the category codes are empty
    * Customer dataset: It combines 1-0 and true-false statements for boolean fields. 
    
### Cleaning Steps

    * Products Dataset: Fields price, rating y view_count are empty
        a) Drop empty columns
    * Event dataset: Most of the category codes are empty
        a) Drop category code column
    * Customer dataset: It combines 1-0 and true-false statements for boolean fields. 
        a) Standardize boolean values to has only the option to be 1 or 0
        
## Step 3: Define the Data Model
### Conceptual Data Model

It is a star schema that consists of three-dimensional tables and one fact table. The fact table is "Product Sales" and the dimensional tables are "Time", "Products" and "Users". 

<img src="data model.png">

This star schema was chosen because of its benefits:

    * Query performance: Data team and business users can query efficiently the tables to get faster insights
    
    * Easily understood: Business users can better understand the goal of each table
    
    * Load performance and administration: Reduces the time required to load large amounts of data into tables

### Mapping Out Data Pipelines

The Data pipelines consist of multiple steps:

    1. Storing data from multiple sources and formats in an S3. 
    2. It copies data to three stages for each source: products, customers, and clients. 
    3. Then, it creates a star schema after cleaning and filtering so the analytics team can analyze eCommerce purchases. 
    4. Business users can connect dashboards to these tables, so they can obtain actionable insights to improve revenue in the company.

<img src="Udacity DE capstone project.png">


## Step 4: Run Pipelines to Model the Data

### Create the data model

It is built in the Airflow folder.

## Data Quality Checks

There was implemented three quality checks were in the code:

    1. Compare null key values for the users' table
    2. Compare null key values for the time table
    3. Compare null key values for products table
    
## Data Dictionary

It is included a data dictionary with all fields in the star schema.

<img src="data dictionary.png">

## Step 5: Run Pipelines to Model the Data

### Technology used and reasons

    * How? Using Airflow with Python
    * Reasons for our choice:
        a) Airflow is an open-source workload manager that brings programmatic control 
        b) Airflow API is written in Python, so it is the natural choice when using Airflow.
        c) Airflow has an intuitive interface to monitor executions

### Discussion

1. Clearly state the rationale for the choice of tools and technologies for the project.

    * Airflow provides programmatic control to each execution and aids in intuitive development.
    

2. Propose how often the data should be updated and why.

    * The data should be updated daily as events are generated every day.
    

3. Write a description of how you would approach the problem differently under the following scenarios:

    - The data was increased by 100x.

        i. We can face the problem in two ways:

            a) We can use Amazon Managed Workflows for Apache Airflow that help us scale and upgrade Airflow.

            b) If performance lowers, it is advisable to scale up Redshift, too.


    - The data populates a dashboard that must be updated daily by 7 am every day.

        * We can schedule the pipeline to run every day at 6 am, so we have up-to-day data at 7 am to be consumed.


    - The database needed to be accessed by 100+ people.

        * Redshift is an MPP database that improves fast reads and also has a great cache that will highly enhance repetitive tasks.
