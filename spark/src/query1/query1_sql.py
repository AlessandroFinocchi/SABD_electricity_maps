from pyspark.sql import functions as F
from deps.hdfs_utils import write_results_on_hdfs, exists_on_hdfs
from deps.influxdb_utils import write_results_on_influxdb
from deps.utils import *
from deps import nifi_utils as nr
from time import time

import time

def run(FILE_FORMAT, _, TIMED) -> float:
    spark, sc = get_spark("Query 1 - SQL")

    #----------------------------------------------- Check hdfs ------------------------------------------------#
    it_file = f"hdfs://namenode:54310/data/IT_all.{FILE_FORMAT}"
    se_file = f"hdfs://namenode:54310/data/SE_all.{FILE_FORMAT}"
    result_file = f"hdfs://namenode:54310/data/results/query1_sql.{FILE_FORMAT}"
    view_name = "view1"
    while not exists_on_hdfs(it_file, sc) or not exists_on_hdfs(se_file, sc):
        nr.run_nifi_flow()
        time.sleep(1)

    #--------------------------------------------- Process results ---------------------------------------------#
    start_time = time()

    it_df = spark.read.csv(it_file, header=False, inferSchema=True).toDF(*ORIGINAL_HEADER)
    se_df = spark.read.csv(se_file, header=False, inferSchema=True).toDF(*ORIGINAL_HEADER)

    df = it_df.union(se_df).withColumn(
        YEAR,
        F.year(F.to_timestamp(DATE, ORIGINAL_DATE_FORMAT))
    )

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
    end_time = time()

    #---------------------------------------------- Save results -----------------------------------------------#
    write_results_on_hdfs(result, FILE_FORMAT, result_file)
    write_results_on_influxdb(result, "query1_sql", QUERY1_CONFIG)
    spark.stop()

    return end_time - start_time