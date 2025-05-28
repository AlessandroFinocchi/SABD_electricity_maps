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
    if file_format == CSV:       return spark.read.csv(filepath, header=False, inferSchema=True)
    elif file_format == PARQUET: return spark.read.parquet(filepath)
    else: raise Exception(f"Unsupported file format: {file_format}")

def exists_on_hdfs(path_str:str, sc: SparkContext) -> bool:
    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    path = sc._jvm.org.apache.hadoop.fs.Path(path_str)
    if fs.exists(path):
        print(f"OK: Input file '{path_str}' found in HDFS.")
        return True
    else:
        print(f"ERROR: Input file '{path_str}' not found in HDFS.")
        return False

def store_results_on_hdfs(result, file_format:str, result_file:str):
    result.show()
    if file_format == CSV: result.coalesce(1).write.mode("overwrite").csv(result_file)
    elif file_format == PARQUET: result.coalesce(1).write.mode("overwrite").parquet(result_file)
    else: raise Exception(f"Unsupported file format: {file_format}")
    print(f"Results {result_file} saved successfully.")