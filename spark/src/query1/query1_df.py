from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config import *
from utils import *
import nifi_runner as nr
# from deps.config import *
# from deps. utils import *
# from deps import nifi_runner as nr

import time

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Query 1") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:54310") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')

    #----------------------------------------------- Check hdfs ------------------------------------------------#
    it_file = f"hdfs://namenode:54310/data/IT_all.{FILE_FORMAT}"
    se_file = f"hdfs://namenode:54310/data/SE_all.{FILE_FORMAT}"
    result_file = f"hdfs://namenode:54310/data/results/query1_df.{FILE_FORMAT}"
    while not exists_on_hdfs(it_file, sc) or not exists_on_hdfs(se_file, sc):
        nr.run_nifi_flow()
        time.sleep(1)

    #--------------------------------------------- Process results ---------------------------------------------#
    it_df = spark.read.csv(it_file, header=False, inferSchema=True).toDF(*ORIGINAL_HEADER)
    se_df = spark.read.csv(se_file, header=False, inferSchema=True).toDF(*ORIGINAL_HEADER)

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

    result.show()

    store_results_on_hdfs(result, FILE_FORMAT, result_file)

    spark.stop()
    print("Results saved successfully.")