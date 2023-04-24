import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def clean_data():
    print("Task started")
    spark = SparkSession.builder.appName('MyApp').master("local").getOrCreate()
    for file in glob.glob("./movies_data/*.csv"):
        print("File found: {}".format(file))
        filename = file.split("/")[-1]
        print(filename)
        df = spark.read.option("multiLine", "true").csv(file, inferSchema =True, header = True)
        df = df.drop('director')
        df = df.withColumn('rating', df["rating"].cast(FloatType()))
        df = df.filter(("year not in ('', 'None') and year is not NULL"))
        df = df.filter(("year regexp '[0-9]+'"))
        df = df.withColumn("year", regexp_replace("year", "\(", ""))
        df = df.withColumn("year", regexp_replace("year", "\)", ""))
        df = df.withColumn("year", regexp_replace("year", "\D+", ""))
        df = df.withColumn('year', substring('year', -4, 4))
        df = df.withColumn('year', df["year"].cast(IntegerType()))
        df = df.withColumn('runtime', regexp_replace('runtime', "min", ""))
        df = df.filter("runtime > 0")
        df = df.withColumn("votes", regexp_replace('votes', ",", ""))
        df = df.withColumn('votes', df['votes'].cast(IntegerType()))
        df.toPandas().to_csv('./cleaned_movies_data/{}'.format(filename), index=False)

        print("Data cleaning is done")

def merge_data():
    full_df = None
    spark = SparkSession.builder.appName('MyApp').master("local").getOrCreate()
    for file in glob.glob("./cleaned_movies_data/*.csv"):
        print(file)
        df = spark.read.option("multiLine", "true").csv(file, inferSchema =True, header = True)
        if full_df is None:
            full_df = df
        else:
            full_df = full_df.union(df)
    full_df.toPandas().to_csv('./cleaned_movies_data/all_movies_data.csv', index=False)
    print("Successfully created single csv file for all the data")
