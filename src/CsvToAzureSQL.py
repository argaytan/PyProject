from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

from dotenv import load_dotenv
import os

load_dotenv('dev.env')

conf = SparkConf()  # create the configuration
conf.set("spark.jars", os.environ.get('AZURE_SQL_JDBC_JAR_PATH'))  # set the spark.jars

spark = SparkSession.builder \
    .config(conf=conf) \
    .appName("CSV to Azure SQL") \
    .getOrCreate()

csv_df = spark.read.format("csv") \
    .option("header", "false") \
    .option("inferSchema", "true") \
    .load("/Users/agaytan/Documents/EDUCATION/PYTHON/globalnt/PyProject/csv/hired_employees.csv") \
    .toDF("id", "name", "datetime", "department_id", "job_id")



transformed_df = csv_df

jdbc_url = "jdbc:sqlserver://"+ os.environ.get('AZURE_SQL_HOST') +":1433;database="+ os.environ.get('AZURE_SQL_DATABASE') +";encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
connection_properties = {
    "user": os.environ.get('AZURE_SQL_USER'),
    "password": os.environ.get('AZURE_SQL_PASSWORD'),
    "driver": os.environ.get('AZURE_SQL_DRIVER')
}

transformed_df.write \
    .mode("overwrite") \
    .jdbc(jdbc_url, "common.hired_employees", properties=connection_properties)


spark.stop()
