# ETL Pipeline: User Profiles & Events Integration

## Overview

This project implements a complete ETL (Extract, Transform, Load) pipeline using Python and SQL Server. It handles user profile and activity data from CSV and JSON sources, transforms the data, performs necessary joins and calculations, and finally stores the processed output in Parquet format.

## Features

-  Extracts user profile data from a CSV file
-  Extracts user event data from a JSON file
-  Validates and transforms the raw data
-  Dynamically creates or resets three tables: `user_profiles`, `user_events`, and `user_activity`
-  Inserts cleaned data into SQL Server tables
-  Joins user profiles with user events using `user_id`
-  Extracts relevant details such as `event_type`, `event_timestamp`, `page_url`, and `device`
-  Calculates activity-related metrics
-  Saves final output as a Parquet file for efficient downstream usage

## Files

- `user_profiles.csv`: Contains user details like name, registration date, and location
- `user_events.json`: Contains raw events with metadata in nested structure
- `etl_pipeline.py`: Python script that handles the full ETL process

## Requirements

- Python 3.8+
- pandas
- SQLAlchemy
- pyodbc
- fastparquet

Install dependencies:

```bash
pip install pandas sqlalchemy pyodbc fastparquet
How to Run
Make sure the input files user_profiles.csv and user_events.json are present in the working directory.

Update the SQL Server connection string in the script.

Run the script:

python etl_pipeline.py
The final output will be saved as a .parquet file in the output/ directory.

Highlights
Modular code with clear function separation

Uses classes to encapsulate table creation and deletion

Automatically drops existing SQL tables to ensure clean loading

Handles JSON parsing with nested fields

Saves the final output in an optimized Parquet format

