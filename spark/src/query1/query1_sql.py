from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from deps. utils import *
from deps import nifi_runner as nr

import time

def run(FILE_FORMAT, _):
    spark = SparkSession.builder \
        .appName("Query 1 - SQL") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:54310") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')

    #----------------------------------------------- Check hdfs ------------------------------------------------#
    it_file = f"hdfs://namenode:54310/data/IT_all.{FILE_FORMAT}"
    se_file = f"hdfs://namenode:54310/data/SE_all.{FILE_FORMAT}"
    result_file = f"hdfs://namenode:54310/data/results/query1_sql.{FILE_FORMAT}"
    view_name = "electricity_map"
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

    df.createOrReplaceTempView(view_name)

    result = spark.sql(f"""
        SELECT
          {COUNTRY}, 
          {YEAR},
          ROUND(AVG(`{INTENSITY_DIRECT}`),2) AS `{INTENSITY_DIRECT_AVG}`,
          MIN(`{INTENSITY_DIRECT}`)          AS `{INTENSITY_DIRECT_MIN}`,
          MAX(`{INTENSITY_DIRECT}`)          AS `{INTENSITY_DIRECT_MAX}`,
          ROUND(AVG(`{CARBON_FREE_PERC}`),2) AS `{CARBON_FREE_PERC_AVG}`,
          MIN(`{CARBON_FREE_PERC}`)          AS `{CARBON_FREE_PERC_MIN}`,
          MAX(`{CARBON_FREE_PERC}`)          AS `{CARBON_FREE_PERC_MAX}`
        FROM {view_name}
        GROUP BY {COUNTRY}, {YEAR}
        ORDER BY {COUNTRY}, {YEAR}
    """)

    #---------------------------------------------- Save results -----------------------------------------------#
    store_results_on_hdfs(result, FILE_FORMAT, result_file)
    spark.stop()