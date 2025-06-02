import time

from deps.influxdb_utils import write_results_on_influxdb
from deps.utils import *
from deps.hdfs_utils import write_results_on_hdfs


def run(_: SparkSession, sc:SparkContext, dataset_path: str, FILE_FORMAT, USE_CACHE:bool, TIMED) -> float:
    #--------------------------------------------- Process results ---------------------------------------------#
    result_file1 = f"hdfs://namenode:54310/data/results/query2_rdd_classification.{FILE_FORMAT}" # classification file
    result_file2 = f"hdfs://namenode:54310/data/results/query2_rdd_progress.{FILE_FORMAT}"       # progress during months file
    start_time = time.time()

    rdd = sc.textFile(dataset_path).filter(lambda x: country(x)=="Italy")

    rdd_avg = rdd.map(lambda x: (f'{year(x)}_{month(x)}', (intensity1(x), free_intensity(x), 1))) \
                 .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))               \
                 .map(lambda x: (x[0], (x[1][0] / x[1][2], x[1][1] / x[1][2])))
    rdd_avg = rdd_avg.cache() if USE_CACHE else rdd_avg

    rdd_avg_int_dsc = sc.parallelize(rdd_avg.takeOrdered(5, key=lambda x: -x[1][0])).sortBy(lambda x: x[1][0], ascending=False)
    rdd_avg_int_asc = sc.parallelize(rdd_avg.takeOrdered(5, key=lambda x:  x[1][0])).sortBy(lambda x: x[1][0], ascending=True)
    rdd_avg_cfe_dsc = sc.parallelize(rdd_avg.takeOrdered(5, key=lambda x: -x[1][1])).sortBy(lambda x: x[1][1], ascending=False)
    rdd_avg_cfe_asc = sc.parallelize(rdd_avg.takeOrdered(5, key=lambda x:  x[1][1])).sortBy(lambda x: x[1][1], ascending=True)

    rdd_classification = rdd_avg_int_dsc  \
                  .union(rdd_avg_int_asc) \
                  .union(rdd_avg_cfe_dsc) \
                  .union(rdd_avg_cfe_asc) \
                  .map(lambda x:(x[0], x[1][0], x[1][1])) # -> (date, avg_intensity, avg_cfe)

    rdd_progress = rdd_avg.map(lambda x:(x[0], x[1][0], x[1][1])) \
                          .sortBy(lambda x: x[0]) # -> (date, avg_intensity, avg_cfe)

    if TIMED:
        rdd_classification.collect()
        rdd_progress.collect()
    end_time = time.time()

    #---------------------------------------------- Save results -----------------------------------------------#
    if not TIMED:
        df_res_classification = rdd_classification.toDF(QUERY2_HEADER)
        df_res_progress       = rdd_progress.toDF(QUERY2_HEADER)

        write_results_on_hdfs(df_res_classification, FILE_FORMAT, result_file1)
        write_results_on_hdfs(df_res_progress, FILE_FORMAT, result_file2)
        write_results_on_influxdb(df_res_progress, "query2_rdd", QUERY2_CONFIG)

    return end_time - start_time