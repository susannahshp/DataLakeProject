# Project: Data Lake



## Summary



As a data engineer in a music streaming startup, Sparkify, I was tasked to build an ETL pipeline. I had to extract their data from S3, process the data and transform the data using Spark, and load the data back into S3.


## How to run the Python scripts



There is only one python file in this project called etl.py. To run this file, open your console and type 'python etl.py' command. This command will execute the python file and will get the data from S3 and process the data and load it back to S3 bucket.



## Explanation of the files



#### dl.cfg

This file is a configuration file containing the values of AWS access key ID and AWS secret access key. This file will be read in the etl.py by the configure parser and it will give you access to AWS S3 buckets by running the etl.py file.


#### etl.py

This file is a python file containing functions to create a spark session, process the song data and process the log data. These functions have parameters which are input data and output data. These parameters have the path to S3 buckets. The functions process and transforms the data by reading JSON files from S3 and making it a spark dataframe and extracting columns to create dimensional tables. Once it is done transforming data, it parquets files and loads the data into the output S3 bucket.


