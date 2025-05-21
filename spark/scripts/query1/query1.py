from pyspark.sql import SparkSession

from spark.scripts.query1.config import FORMAT
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
    rdd_map = rdd.map(lambda x: (country(x), year(x), intensity1(x), free_intensity(x)))

