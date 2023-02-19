import boto3
import psycopg2
from datetime import datetime
import configparser
from datetime import datetime

def create_table_for_movies_table(cur):
    cur.execute("""
        create table movies_data(
            title varchar(300) not null,
            year int not null,
            certificate varchar not null,
            runtime int not null,
            genre varchar not null,
            rating float not null,
            votes int not null)
            sortkey (year);
    """)

def create_table_for_series_table(cur):
    cur.execute("""
        create table series_data(
            title varchar(300) not null,
            certificate varchar not null,
            runtime int not null,
            genre varchar not null,
            rating float not null,
            votes int not null,
            start_year int not null,
            end_year int not null)
            sortkey (start_year);
    """)

def copy_data_to_movies_table(cur, file):
    cur.execute("""
        copy movies_data (title, year, certificate, runtime, genre, rating, votes) from 's3://learning-de/{}'
        credentials ''
        DELIMITER '|'
        IGNOREHEADER 1
        format as csv
    """.format(file))

def copy_data_to_series_table(cur, file):
    cur.execute("""
        copy series_data (title, certificate, runtime, genre, rating, votes, start_year, end_year) from 's3://learning-de/{}'
        credentials ''
        DELIMITER '|'
        IGNOREHEADER 1
        format as csv
    """.format(file))

def insert(data_type):
    #data_type = ti.xcom_pull(key="s3_data_type")
    print("Task started")
    config=configparser.ConfigParser()
    config.read_file(open('./cluster.config'))
    KEY = config.get("AWS", "KEY")
    SECRET = config.get("AWS", "SECRET")
    DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    DWH_DB = config.get("DWH", "DWH_DB")
    DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH", "DWH_PORT")
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
    DWH_CLUSTER_PORT = config.get("DWH", "DWH_PORT")
    print("Config Loaded")

    s3 = boto3.resource('s3', region_name = "ap-south-1", aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    iam = boto3.client('iam', aws_access_key_id=KEY, aws_secret_access_key=SECRET, region_name="ap-south-1")
    redshift = boto3.client('redshift', region_name="ap-south-1", aws_access_key_id=KEY, aws_secret_access_key=SECRET)

    bucket=s3.Bucket("learning-de")
    today = datetime.today().strftime('%Y%m%d')
    objs = bucket.objects.filter(Prefix="{}_data/{}/".format(data_type, today))
    files = [i.key for i in objs]
    if not len(files) > 0:
        print("No files found in the path - {}_data/{}/".format(data_type, today))
    elif len(files) > 1:
        print("More than 1 file found in the path - {}_data/{}/".format(data_type, today))

    print("File found in S3: {}".format(files[0]))

    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)["Role"]["Arn"]

    response = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
    print(response)

    current_cluster = response['Clusters'][0]
    DWH_ENDPOINT = current_cluster['Endpoint']['Address']
    DWH_ROLE_ARN = current_cluster['IamRoles'][0]['IamRoleArn']
    DB_NAME = current_cluster['DBName']
    DB_USER = current_cluster['MasterUsername']

    attempts = 0
    while attempts < 3:
        attempts += 1
        try:
            conn = psycopg2.connect(host=DWH_ENDPOINT, dbname=DB_NAME, user=DB_USER, password=DWH_DB_PASSWORD, port=DWH_CLUSTER_PORT)
        except Exception as e:
            print("Error: {}".format(e))
            print("retry attempt: {}".format(attempts))
            if attempts == 3:
                exit

    conn.set_session(autocommit=True)

    try:
        cur = conn.cursor()
    except Exception as e:
        print(e)
        exit

    if data_type == "movies":
        create_table_for_movies_table(cur)
    else:
        create_table_for_series_table(cur)

    if data_type == "movies":
        copy_data_to_movies_table(cur, files[0])
    else:
        copy_data_to_series_table(cur, files[0])
    
"""
    cur.execute("
        select count(*) from movies_data;
    ")

    res = cur.fetchone()
    if type(res) == tuple and res[0] > 0:
        print("Successfully inserted the data")
    else:
        print("Failed to insert the data")
"""

def insert_movies_data():
    insert("movies")
    
def insert_series_data():
    insert("series")
    