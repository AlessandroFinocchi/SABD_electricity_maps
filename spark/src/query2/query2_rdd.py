from deps.utils import *
from deps import nifi_utils as nr
from deps.hdfs_utils import write_results_on_hdfs, exists_on_hdfs

import time


def run(FILE_FORMAT, USE_CACHE):
    spark, sc = get_spark("Query 2 - RDD")

    #----------------------------------------------- Check hdfs ------------------------------------------------#
    it_file = f"hdfs://namenode:54310/data/IT_all.{FILE_FORMAT}"
    result_file1 = f"hdfs://namenode:54310/data/results/query2_rdd_classification.{FILE_FORMAT}" # classification file
    result_file2 = f"hdfs://namenode:54310/data/results/query2_rdd_progress.{FILE_FORMAT}"       # progress during months file

    while not exists_on_hdfs(it_file, sc):
        nr.run_nifi_flow()
        time.sleep(1)

    #--------------------------------------------- Process results ---------------------------------------------#
    rdd = sc.textFile(it_file)

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

    #---------------------------------------------- Save results -----------------------------------------------#
    df_res1 = rdd_classification.toDF(QUERY2_HEADER)
    df_res2 = rdd_progress.toDF(QUERY2_HEADER)
    write_results_on_hdfs(df_res1, FILE_FORMAT, result_file1)
    write_results_on_hdfs(df_res2, FILE_FORMAT, result_file2)

    spark.stop()