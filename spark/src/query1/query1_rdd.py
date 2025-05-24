from pyspark.sql import SparkSession
from ..deps.config import *
from ..deps.utils import *
from ..deps import nifi_runner as nr

import time


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Query 1") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:54310") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')

    #----------------------------------------------- Check hdfs ------------------------------------------------#
    it_file = f"hdfs://namenode:54310/data/IT_all.{FORMAT}"
    se_file = f"hdfs://namenode:54310/data/SE_all.{FORMAT}"
    while not exists_on_hdfs(it_file, sc) or not exists_on_hdfs(se_file, sc):
        nr.run_nifi_flow()
        time.sleep(1)

    #--------------------------------------------- Process results ---------------------------------------------#
    it_rdd = sc.textFile(it_file)
    se_rdd = sc.textFile(se_file)

    rdd = it_rdd.union(se_rdd)
    rdd_map = rdd.map(lambda x: ((country(x), year(x)), (intensity1(x), free_intensity(x), 1)))
    if USE_CACHE: rdd_map.cache()

    rdd_extremes = rdd_map.reduceByKey(lambda x, y: (min(x[0], y[0]),
                                                     min(x[1], y[1]),
                                                     max(x[0], y[0]),
                                                     max(x[1], y[1])
                                                     )
                                       )
    rdd_avg = rdd_map.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])) \
                     .map(lambda x: (x[0], (x[1][0] / x[1][2], x[1][1] / x[1][2])))

    rdd_results = rdd_extremes.join(rdd_avg) \
                              .mapValues(lambda pair: pair[0] + pair[1]) \
                              .sortByKey()

    #---------------------------------------------- Save results -----------------------------------------------#
    rdd_results.coalesce(1) \
               .saveAsTextFile(f"hdfs://namenode:54310/data/results/query1_rdd.{FORMAT}")
    spark.stop()