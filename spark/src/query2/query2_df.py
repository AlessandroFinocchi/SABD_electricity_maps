from pyspark.sql import SparkSession
from deps.utils import *
from deps import nifi_runner as nr

import time


def run(FILE_FORMAT, USE_CACHE):
    spark, sc = get_spark("Query 2 - DF")

    #----------------------------------------------- Check hdfs ------------------------------------------------#
    it_file = f"hdfs://namenode:54310/data/IT_all.{FILE_FORMAT}"
    result_file1 = f"hdfs://namenode:54310/data/results/query2_df_classification.{FILE_FORMAT}" # classification file
    result_file2 = f"hdfs://namenode:54310/data/results/query2_df_progress.{FILE_FORMAT}"       # progress during months file

    while not exists_on_hdfs(it_file, sc):
        nr.run_nifi_flow()
        time.sleep(1)

    #--------------------------------------------- Process results ---------------------------------------------#
    df = get_df(spark, it_file, FILE_FORMAT)

    #TODO: CONTINUAAAAA

    #---------------------------------------------- Save results -----------------------------------------------#
    store_results_on_hdfs(df_res1, FILE_FORMAT, result_file1)
    store_results_on_hdfs(df_res2, FILE_FORMAT, result_file2)

    spark.stop()