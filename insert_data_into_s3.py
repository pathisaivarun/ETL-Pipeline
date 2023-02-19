import boto3
from datetime import datetime
import configparser

def insert(data_type):
    #data_type = ti.xcom_pull(key="data_type")
    print("Task Started")
    config=configparser.ConfigParser()
    config.read_file(open('./cluster.config'))
    KEY = config.get("AWS", "KEY")
    SECRET = config.get("AWS", "SECRET")
    print("Config Loaded")
    
    session = boto3.Session(
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )

    s3 = session.resource('s3')
    bucket = s3.Bucket('learning-de')
    today = datetime.today().strftime('%Y%m%d')
    with open('./cleaned_{}_data/all_{}_data.csv'.format(data_type, data_type), 'rb') as f:
        bucket.put_object(Body=f, Bucket='learning-de', Key='{}_data/{}/all_{}_data.csv'.format(data_type, today, data_type))
    
    print("Successfully inserted the data into s3 bucket") 

def insert_movies_data():
    insert("movies")
    #ti.xcom_push(key="s3_data_type", value="movies")

def insert_series_data():
    insert("series")
    #ti.xcom_push(key="s3_data_type", value="series")
