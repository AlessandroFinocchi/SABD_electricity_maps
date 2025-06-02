import time

from pyspark.sql import functions as F
from pyspark.sql.functions import col
from deps.hdfs_utils import write_results_on_hdfs
from deps.influxdb_utils import write_results_on_influxdb
from deps.utils import *


def run(spark: SparkSession, _1:SparkContext, dataset_path: str, FILE_FORMAT, _2: bool, TIMED) -> float:
    #--------------------------------------------- Process results ---------------------------------------------#
    result_file1 = f"hdfs://namenode:54310/data/results/query2_df_classification.{FILE_FORMAT}" # classification file
    result_file2 = f"hdfs://namenode:54310/data/results/query2_df_progress.{FILE_FORMAT}"       # progress during months file
    start_time = time.time()

    df_progress = get_df(spark, dataset_path, FILE_FORMAT) \
        .filter(col(COUNTRY)=="Italy")\
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
    end_time = time.time()

    #---------------------------------------------- Save results -----------------------------------------------#
    if not TIMED:
        write_results_on_hdfs(df_classification, FILE_FORMAT, result_file1)
        write_results_on_hdfs(df_progress, FILE_FORMAT, result_file2)
        write_results_on_influxdb(df_progress, "query2_df", QUERY2_CONFIG)

    return end_time - start_time