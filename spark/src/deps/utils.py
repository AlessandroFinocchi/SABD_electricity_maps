from pyspark import SparkContext
from deps.config import *

def year(rdd) -> int: return rdd.split(",")[0].split("-")[0]

def country(rdd) -> str: return rdd.split(",")[1]

def intensity1(rdd) -> float: return float(rdd.split(",")[4])

def free_intensity(rdd) -> float: return float(rdd.split(",")[6])

def pretty_collect(rdd):
    for result in rdd.collect():
        print(result)

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

def store_results_on_hdfs(result, format:str, result_file:str):
    result.show()
    if format == CSV: result.coalesce(1).write.mode("overwrite").csv(result_file)
    elif format == PARQUET: result.coalesce(1).write.mode("overwrite").parquet(result_file)
    else: raise Exception(f"Unsupported file format: {format}")
    print("Results saved successfully.")