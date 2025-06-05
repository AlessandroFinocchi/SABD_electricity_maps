import time

from pyspark.sql import functions as F
from deps.hdfs_utils import write_results_on_hdfs
from deps.influxdb_utils import write_results_on_influxdb
from deps.utils import *


def run(spark: SparkSession, _: SparkContext, dataset_path: str,  FILE_FORMAT, TIMED) -> float:

    #--------------------------------------------- Process results ---------------------------------------------#
    result_file = f"hdfs://namenode:54310/data/results/query1_df.{FILE_FORMAT}"
    start_time = time.time()

    df = get_df(spark, dataset_path, FILE_FORMAT).withColumn(
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