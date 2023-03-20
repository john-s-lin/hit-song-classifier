import csv
import numpy as np
import os
import sys
# Spark imports
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import desc
# Dask imports
import dask.bag as db
import dask.dataframe as df  # you can use Dask bags or dataframes
from csv import reader


def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark


# Useful functions to print RDDs and Dataframes.
def toCSVLineRDD(rdd):
    '''
    This function convert an RDD or a DataFrame into a CSV string
    '''
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row])) \
        .reduce(lambda x, y: os.linesep.join([x, y]))
    return a + os.linesep


def toCSVLine(data):
    '''
    Convert an RDD or a DataFrame into a CSV string
    '''
    if isinstance(data, RDD):
        return toCSVLineRDD(data)
    elif isinstance(data, DataFrame):
        return toCSVLineRDD(data.rdd)
    return None


def intersection_datasets():
    spark = init_spark()

    df1 = spark.read.csv("./data/classified_billboard_songs1.csv", header=True, mode="DROPMALFORMED")

    df2 = spark.read.csv("./data/million_songs.csv", header=True, mode="DROPMALFORMED")
    df2 = df2.withColumnRenamed("artist_name", "artist") \
        .withColumnRenamed("title", "song")

    df3 = df1.join(df2, ['artist', 'song'])
    return_value = toCSVLine(df3).replace("\r\n", "\n")

    # Write to CSV
    df3.write.csv("./data/million_songs_billboard_join.csv")


intersection_datasets()
