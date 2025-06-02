from pyspark.sql import functions as F
from deps.hdfs_utils import write_results_on_hdfs
from deps.influxdb_utils import write_results_on_influxdb
from deps.utils import *

import time

def run(spark: SparkSession, _1: SparkContext, dataset_path: str, FILE_FORMAT, _2: bool, TIMED) -> float:

    #--------------------------------------------- Process results ---------------------------------------------#
    result_file = f"hdfs://namenode:54310/data/results/query1_sql.{FILE_FORMAT}"
    view_name = "view1"
    start_time = time.time()

    df = spark.read\
              .csv(dataset_path, header=False, inferSchema=True)\
              .toDF(*ORIGINAL_HEADER) \
              .withColumn(YEAR, F.year(F.to_timestamp(DATE, ORIGINAL_DATE_FORMAT)))

    df.createOrReplaceTempView(view_name)

    result = spark.sql(f"""
        SELECT
          {YEAR},
          {COUNTRY}, 
          ROUND(AVG(`{INTENSITY_DIRECT}`), 6) AS `{INTENSITY_DIRECT_AVG}`,
          ROUND(MIN(`{INTENSITY_DIRECT}`), 6) AS `{INTENSITY_DIRECT_MIN}`,
          ROUND(MAX(`{INTENSITY_DIRECT}`), 6) AS `{INTENSITY_DIRECT_MAX}`,
          ROUND(AVG(`{CARBON_FREE_PERC}`), 6) AS `{CARBON_FREE_PERC_AVG}`,
          ROUND(MIN(`{CARBON_FREE_PERC}`), 6) AS `{CARBON_FREE_PERC_MIN}`,
          ROUND(MAX(`{CARBON_FREE_PERC}`), 6) AS `{CARBON_FREE_PERC_MAX}`
        FROM {view_name}
        GROUP BY {YEAR}, {COUNTRY}
        ORDER BY {COUNTRY}, {YEAR}
    """)

    if TIMED: result.collect()
    end_time = time.time()

    #---------------------------------------------- Save results -----------------------------------------------#
    if not TIMED:
        write_results_on_hdfs(result, FILE_FORMAT, result_file)
        write_results_on_influxdb(result, "query1_sql", QUERY1_CONFIG)

    return end_time - start_time