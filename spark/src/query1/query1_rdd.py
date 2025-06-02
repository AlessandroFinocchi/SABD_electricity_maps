import time

from deps.hdfs_utils import write_results_on_hdfs
from deps.influxdb_utils import write_results_on_influxdb
from deps.utils import *


def run(_: SparkSession, sc:SparkContext, dataset_path: str,  FILE_FORMAT, USE_CACHE, TIMED) -> float:
    #--------------------------------------------- Process results ---------------------------------------------#
    result_file = f"hdfs://namenode:54310/data/results/query1_rdd.{FILE_FORMAT}"
    start_time = time.time()

    rdd = sc.textFile(dataset_path)
    rdd_map = rdd.map(lambda x: ((country(x), year(x)), (intensity1(x), free_intensity(x), 1)))
    rdd_map = rdd_map.cache() if USE_CACHE else rdd_map

    rdd_min = rdd_map.reduceByKey(lambda x, y: (min(x[0], y[0]), min(x[1], y[1])))
    rdd_max = rdd_map.reduceByKey(lambda x, y: (max(x[0], y[0]), max(x[1], y[1])))
    rdd_avg = rdd_map.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])) \
                     .map(lambda x: (x[0], (x[1][0] / x[1][2], x[1][1] / x[1][2])))

    rdd_results = rdd_min.join(rdd_avg) \
                         .join(rdd_max) \
                         .sortByKey() \
                         .map(lambda x:(
                                        x[0][1],        # year
                                        x[0][0],        # country
                                        x[1][0][1][0],  # avg intensity
                                        x[1][0][0][0],  # min intensity
                                        x[1][1][0],     # max intensity
                                        x[1][0][1][1],  # avg cfe
                                        x[1][0][0][1],  # min cfe
                                        x[1][1][1]      # max cfe
                                        )
                              )

    if TIMED: rdd_results.collect()
    end_time = time.time()

    #---------------------------------------------- Save results -----------------------------------------------#
    if not TIMED:
        df_res = rdd_results.toDF(QUERY1_HEADER)
        write_results_on_hdfs(df_res, FILE_FORMAT, result_file)
        write_results_on_influxdb(df_res, "query1_rdd", QUERY1_CONFIG)

    return end_time - start_time