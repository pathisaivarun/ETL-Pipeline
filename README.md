# ETL Pipeline

In this project, I have created a small pipeline which will do
1) Data collection
2) Data processing
3) Data ingestion

Here, I have used airflow for scheduling and managing the tasks.


**Data collection**

I have data about movies and TV-series which i have scraped previously.
So, I just saved this data into a folder and loaded into pandas dataframe


**Data processing**

I have done the below processing to make sure the data is cleaned and useful
1) Removed the useless columns 
2) Removed the rows where the year column is empty or None
3) Removed the empty spaces around the text in each column
4) Converted the data in runtime column to minutes only
  Ex: Before: 43 min
      After: 43
5) Cleaned votes and ratings columns
6) Converted the columns from text data type to int/float/text data types.
For this cleaning i have mostly used lambda functions, replace with string/regex, contains function etc.


**Data ingestion**

I wanted to use redshift database as my final database.
1) After cleaning the data, i have created a single file out of all the multiple files (This will cause memory issue for big files. Better to process multiple small files only)
2) And uploaded the files into S3 bucket using access point. (As S3 is cheap, we can keep the data here for sometime and use it as backup incase of data loss)
3) From S3, i have uploaded the data to redshift database using IAM role. (I have used the year/start_year column as sortkey so that the quering speed will be better.)


**Challenges faced:**
1) Intially, i thought of using parquet file format as the compressed file size will be very less compared to CSV. But i have faced a lot of difficulties and also observed the csv file size is a bit less than the parquet file. So, continued with csv file format only.
2) I tried using EC2 instance but freetier version cannot install airflow due to memory issues.
3) I have tried airflow xcom database to pass the parameters b/w tasks but it didn't worked well. 


**Airflow DAG**
![airflow](https://user-images.githubusercontent.com/54261591/219932171-151ab033-c6d6-4963-a9ed-fe90227d5a91.png)

1) I have used taskgroups to make sure that 2 pipelines run in parallel
2) Used common methods for uploaded data into S3 and redshift.
