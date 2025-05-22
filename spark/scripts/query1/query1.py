from pyspark.sql import SparkSession

from spark.scripts.query1.config import *
from spark.scripts.utils import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Query 1") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:54310") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')

    # read files
    it_file = f"hdfs://namenode:54310/data/IT_all.{FORMAT}"
    se_file = f"hdfs://namenode:54310/data/SE_all.{FORMAT}"
    it_rdd = sc.textFile(it_file)
    se_rdd = sc.textFile(se_file)

    # remove header lines
    it_header = it_rdd.first()
    se_header = se_rdd.first()
    it_rdd.filter(lambda row: row != it_header)
    se_rdd.filter(lambda row: row != se_header)

    rdd = it_rdd.union(se_rdd)
    rdd_map = rdd.map(lambda x: ((country(x), year(x)), (intensity1(x), free_intensity(x), 1)))
    if USE_CACHE: rdd_map.cache()

    rdd_min = rdd.reduceByKey(lambda x, y: (min(x[0], y[0]), min(x[1], y[1])))
    rdd_max = rdd.reduceByKey(lambda x, y: (max(x[0], y[0]), max(x[1], y[1])))
    rdd_avg = rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))\
                 .map(lambda x: (x[0], (x[1][0] / x[2], x[1][1] / x[2])))

