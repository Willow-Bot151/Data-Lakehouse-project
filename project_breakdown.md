# Coordinate AWS
- One IAM User?
- Credentials through github secret?
# Coordinate Repo 
- forking and merging
- REEQUIREMENTS, python version
- makefile
- dotenv
- READ ME
## file structure 
### folder per week?
#### src
- MVC
#### test
- unit tests on utils and handlers
- integration tests
#### terraform
# Coding
## Pair progrmamming
- 5 people so mobbing
- solo people can review code
- make sure people can move about
## TDD
- writing test files for python concurrently to writing the code.
- error handling
## Reviews
- fresh eyes
- single person reviews
- review asap when someone becomes free from a ticket
- dedicated daily review time, maybe after lunch. 
- review tests, formatting, readability, docstrings
- make comments and let the writer make changes, constructive crititism. or maybe write a test for a missing behaviour.

## Trelo Kanban
- splitting up the task into sprints and making tickets.

# Ingestion
Using an eventbridge trigger, a lambda function
## Terraform for lambda and s3 bucket with monitoring
### s3
- s3 bucket creation
- relevant IAM stuff
- cloudwatch
### Lambda
- lambda creation
- cloudwatch
- eventbridge
- relevant IAM stuff

## Lambda intake function
on trigger of new data / timer. pull new data from db with query
### SQL queries utils
- functions to implement queries from ToteSys
- intaking all data from all tables individually
### Lambda handler
- writen and deployed 
- automatically query new data
- deposit to bucket as new files
- logging
### s3 file format
- new entries as new files
- keys should include: table its coming from, unique id, timestamp, seperated by some special char for pattern recognition 
- JSON files
### (EXTENTION) intake from other sources?


# Processing
take JSON files
## Terraform for lambdas from ingestion and to warehouse, s3 bucket, monitoring
### ingestion
#### s3
- s3 bucket creation
- relevant IAM stuff
- cloudwatch
#### Lambda
- lambda creation
- cloudwatch
- eventbridge
- relevant IAM stuff

### warehousing
#### ?
- relevant IAM stuff
- cloudwatch
#### Lambda
- lambda creation
- cloudwatch
- eventbridge
- relevant IAM stuff

## Lambda transformation between buckets
- pandas / polars
- clean data
- transform consistant format
- Parquet
## Lambda to input data
- star schema
- SQL Inserts


# Warehouse / Clean up / Present
## Presentation
## Documentation
- how to use
- features
- how it works