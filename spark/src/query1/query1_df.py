from pyspark.sql import functions as F
from deps.hdfs_utils import write_results_on_hdfs, exists_on_hdfs
from deps.influxdb_utils import write_results_on_influxdb
from deps.utils import *
from deps import nifi_utils as nr

import time

def run(spark: SparkSession, sc:SparkContext, FILE_FORMAT, _, TIMED) -> float:

    #----------------------------------------------- Check hdfs ------------------------------------------------#
    it_file = f"hdfs://namenode:54310/data/IT_all.{FILE_FORMAT}"
    se_file = f"hdfs://namenode:54310/data/SE_all.{FILE_FORMAT}"
    result_file = f"hdfs://namenode:54310/data/results/query1_df.{FILE_FORMAT}"
    while not exists_on_hdfs(it_file, sc) or not exists_on_hdfs(se_file, sc):
        nr.run_nifi_flow()
        time.sleep(1)

    #--------------------------------------------- Process results ---------------------------------------------#
    start_time = time.time()

    it_df = get_df(spark, it_file, FILE_FORMAT)
    se_df = get_df(spark, se_file, FILE_FORMAT)

    df = it_df.union(se_df).withColumn(
        YEAR,
        F.year(F.to_timestamp(DATE, ORIGINAL_DATE_FORMAT))
    )

    result = df.groupby(YEAR, COUNTRY) \
               .agg(F.avg(INTENSITY_DIRECT).alias(INTENSITY_DIRECT_AVG),
                    F.min(INTENSITY_DIRECT).alias(INTENSITY_DIRECT_MIN),
                    F.max(INTENSITY_DIRECT).alias(INTENSITY_DIRECT_MAX),
                    F.avg(CARBON_FREE_PERC).alias(CARBON_FREE_PERC_AVG),
                    F.min(CARBON_FREE_PERC).alias(CARBON_FREE_PERC_MIN),
                    F.max(CARBON_FREE_PERC).alias(CARBON_FREE_PERC_MAX)) \
               .orderBy(COUNTRY, YEAR)

    if TIMED: result.collect()
    end_time = time.time()

    #---------------------------------------------- Save results -----------------------------------------------#
    if not TIMED:
        write_results_on_hdfs(result, FILE_FORMAT, result_file)
        write_results_on_influxdb(result, "query1_df", QUERY1_CONFIG)

    return end_time - start_time