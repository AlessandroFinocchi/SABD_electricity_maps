from typing import Tuple
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from deps.config import *

def get_spark(appName: str) -> Tuple[SparkSession, SparkContext] :
    spark = SparkSession.builder \
        .appName(appName) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:54310") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')

    return spark, sc

def country(rdd) -> str: return rdd.split(",")[1]

def year(rdd) -> int: return rdd.split(",")[0].split("-")[0]

def month(rdd) -> int: return rdd.split(",")[0].split("-")[1]

def intensity1(rdd) -> float: return float(rdd.split(",")[4])

def free_intensity(rdd) -> float: return float(rdd.split(",")[6])

def pretty_collect(rdd):
    for result in rdd.collect():
        print(result)

def get_df(spark: SparkSession, filepath: str, file_format:str) -> DataFrame:
    if file_format == CSV:       df = spark.read.csv(filepath, header=False, inferSchema=True)
    elif file_format == PARQUET: df = spark.read.parquet(filepath)
    else: raise Exception(f"Unsupported file format: {file_format}")
    return df.toDF(*ORIGINAL_HEADER)