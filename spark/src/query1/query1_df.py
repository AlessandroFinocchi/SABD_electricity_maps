from pyspark.sql import functions as F
from deps.utils import *
from deps import nifi_runner as nr

import time

def run(FILE_FORMAT, _):
    spark, sc = get_spark("Query 1 - DF")

    #----------------------------------------------- Check hdfs ------------------------------------------------#
    it_file = f"hdfs://namenode:54310/data/IT_all.{FILE_FORMAT}"
    se_file = f"hdfs://namenode:54310/data/SE_all.{FILE_FORMAT}"
    result_file = f"hdfs://namenode:54310/data/results/query1_df.{FILE_FORMAT}"
    while not exists_on_hdfs(it_file, sc) or not exists_on_hdfs(se_file, sc):
        nr.run_nifi_flow()
        time.sleep(1)

    #--------------------------------------------- Process results ---------------------------------------------#
    it_df = get_df(spark, it_file, FILE_FORMAT).toDF(*ORIGINAL_HEADER)
    se_df = get_df(spark, se_file, FILE_FORMAT).toDF(*ORIGINAL_HEADER)

    df = it_df.union(se_df).withColumn(
        YEAR,
        F.year(F.to_timestamp(DATE, DATE_FORMAT))
    )

    result = df.groupby(COUNTRY, YEAR) \
               .agg(F.min(INTENSITY_DIRECT).alias(INTENSITY_DIRECT_MIN),
                    F.avg(INTENSITY_DIRECT).alias(INTENSITY_DIRECT_AVG),
                    F.max(INTENSITY_DIRECT).alias(INTENSITY_DIRECT_MAX),
                    F.min(CARBON_FREE_PERC).alias(CARBON_FREE_PERC_MIN),
                    F.avg(CARBON_FREE_PERC).alias(CARBON_FREE_PERC_AVG),
                    F.max(CARBON_FREE_PERC).alias(CARBON_FREE_PERC_MAX)) \
               .orderBy(COUNTRY, YEAR)

    #---------------------------------------------- Save results -----------------------------------------------#
    store_results_on_hdfs(result, FILE_FORMAT, result_file)
    spark.stop()