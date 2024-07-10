# multinational-retail-data-centralisation85

## Project Description

The aim of this project is to clean and centralise multinational retail data to facilitate data analysis. 
Data sources include: relational database, S3 buckets, APIs and CSV files. 

## File Structure

Key Python files are:

- database utils: here we create a connection to dbs including local db that hosts and centralises all the multionational retail data
- Data extraction: here we extract data from sources and convert it to Pandas dataframes
- Data cleaning: here we clean the dataframes provided by data extraction and prepare them for upload to the local Postgres db
- Multinational retail queries: contains the SQL queries to fetch data from the star-schema Postgres database filled with data cleaned with the data cleaning file and uploaded via the data connector in the databsase utils file

## Usage Instructions

1. Set up a Postgres db locally and store its creds in a yaml file
2. Use the database connector class to connect to your local db
3. Fetch data from the various AWS sources using the data extraction class
4. Clean the data with the data cleaning class 
5. Upload cleaned up data to local database with connector class db upload method
6. Complete the star schema, adding primary keys and changing column data types in local db
7. Run the sql queries in multinational_retail_queries to obtain the required sales insights

