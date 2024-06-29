# multinational-retail-data-centralisation85

## Project Description

The aim of this project is to clean and centralise multinational retail data to facilitate data analysis. 
Data sources include: relational database, S3 buckets, APIs and CSV files. 

## File Structure

Key Python files are:

- database utils: here we create a connection to dbs including local db that hosts and centralises all the multionational retail data
- Data extraction: here we extract data from sources and convert it to Pandas dataframes
- Data cleaning: here we clean the dataframes provided by data extraction and prepare them for upload to the local Postgres db

