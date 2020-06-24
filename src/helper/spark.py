from pyspark.sql import SparkSession


def create_spark_session(name):
    spark = SparkSession.builder.master('local').appName(name).getOrCreate()
    
    return spark
