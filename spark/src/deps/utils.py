import time

from typing import Tuple
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from deps.config import *
from deps.hdfs_utils import exists_on_hdfs
from deps import nifi_utils as nr


def get_spark(appName: str) -> Tuple[SparkSession, SparkContext] :
    spark = SparkSession.builder \
        .appName(appName) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:54310") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')

    return spark, sc

def country(rdd_elem)        -> str:   return rdd_elem.split(",")[1]

def year(rdd_elem)           -> int:   return int(rdd_elem.split(",")[0].split("-")[0])

def month(rdd_elem)          -> str:   return rdd_elem.split(",")[0].split("-")[1]

def intensity1(rdd_elem)     -> float: return float(rdd_elem.split(",")[2])

def free_intensity(rdd_elem) -> float: return float(rdd_elem.split(",")[3])

def pretty_collect(rdd_elem):
    for result in rdd_elem.collect():
        print(result)

def get_df(spark: SparkSession, filepath: str, file_format:str) -> DataFrame:
    if file_format == CSV:       df = spark.read.csv(filepath, header=False, inferSchema=True)
    elif file_format == PARQUET: df = spark.read.parquet(filepath)
    else: raise Exception(f"Unsupported file format: {file_format}")
    return df.toDF(*ORIGINAL_HEADER)

def check_hdfs(sc: SparkContext, file_path: str, file_format:str):
    while not exists_on_hdfs(file_path, sc):
        try:
            nr.run_nifi_flow()
            time.sleep(5)
        except Exception:
            print(f"NiFi not up yet, waiting 5s…")
            time.sleep(5)
    nr.stop_nifi_flow()