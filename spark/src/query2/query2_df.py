from pyspark.sql import functions as F
from pyspark.sql.functions import col
from deps.hdfs_utils import write_results_on_hdfs, exists_on_hdfs
from deps.influxdb_utils import write_results_on_influxdb
from deps.utils import *
from deps import nifi_utils as nr
from time import time

import time


def run(FILE_FORMAT, _, TIMED) -> float:
    spark, sc = get_spark("Query 2 - DF")

    #----------------------------------------------- Check hdfs ------------------------------------------------#
    it_file = f"hdfs://namenode:54310/data/IT_all.{FILE_FORMAT}"
    result_file1 = f"hdfs://namenode:54310/data/results/query2_df_classification.{FILE_FORMAT}" # classification file
    result_file2 = f"hdfs://namenode:54310/data/results/query2_df_progress.{FILE_FORMAT}"       # progress during months file

    while not exists_on_hdfs(it_file, sc):
        nr.run_nifi_flow()
        time.sleep(1)

    #--------------------------------------------- Process results ---------------------------------------------#
    start_time = time()

    df_progress = get_df(spark, it_file, FILE_FORMAT) \
        .withColumn(YEAR_MONTH, F.date_format(F.to_timestamp(DATE, ORIGINAL_DATE_FORMAT), "yyyy_MM")) \
        .select(YEAR_MONTH, INTENSITY_DIRECT, CARBON_FREE_PERC) \
        .groupby(YEAR_MONTH) \
        .agg(
             F.avg(INTENSITY_DIRECT).alias(INTENSITY_DIRECT_AVG),
             F.avg(CARBON_FREE_PERC).alias(CARBON_FREE_PERC_AVG),
        ) \
        .orderBy(YEAR_MONTH)

    df_avg_int_dsc = df_progress.orderBy(col(INTENSITY_DIRECT_AVG).desc()).limit(5)
    df_avg_int_asc = df_progress.orderBy(col(INTENSITY_DIRECT_AVG).asc()).limit(5)
    df_avg_cfe_dsc = df_progress.orderBy(col(CARBON_FREE_PERC_AVG).desc()).limit(5)
    df_avg_cfe_asc = df_progress.orderBy(col(CARBON_FREE_PERC_AVG).asc()).limit(5)

    df_classification = df_avg_int_dsc  \
                 .union(df_avg_int_asc) \
                 .union(df_avg_cfe_dsc) \
                 .union(df_avg_cfe_asc)

    if TIMED:
        df_classification.collect()
        df_progress.collect()
    end_time = time()

    #---------------------------------------------- Save results -----------------------------------------------#
    write_results_on_hdfs(df_classification, FILE_FORMAT, result_file1)
    write_results_on_hdfs(df_progress, FILE_FORMAT, result_file2)
    write_results_on_influxdb(df_progress, "query2_df", QUERY2_CONFIG)
    spark.stop()

    return end_time - start_time