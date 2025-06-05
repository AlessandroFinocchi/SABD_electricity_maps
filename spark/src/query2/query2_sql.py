import time

from pyspark.sql import functions as F
from deps.influxdb_utils import write_results_on_influxdb
from deps.utils import *
from deps.hdfs_utils import write_results_on_hdfs


def run(spark: SparkSession, _:SparkContext, dataset_path: str, FILE_FORMAT, TIMED) -> float:
    #--------------------------------------------- Process results ---------------------------------------------#
    result_file1 = f"hdfs://namenode:54310/data/results/query2_sql_classification.{FILE_FORMAT}" # classification file
    result_file2 = f"hdfs://namenode:54310/data/results/query2_sql_progress.{FILE_FORMAT}"       # progress during months file
    view_name     = "view1"
    view_name_avg = f"{view_name}_avg"
    start_time = time.time()

    df = get_df(spark, dataset_path, FILE_FORMAT) \
        .withColumn(YEAR_MONTH, F.date_format(F.to_timestamp(DATE, ORIGINAL_DATE_FORMAT), "yyyy_MM")) \

    df.createOrReplaceTempView(view_name)

    df_progress = spark.sql(f"""
        SELECT 
            {YEAR_MONTH}, 
            ROUND(AVG(`{INTENSITY_DIRECT}`), 6) AS `{INTENSITY_DIRECT_AVG}`, 
            ROUND(AVG(`{CARBON_FREE_PERC}`), 6) AS `{CARBON_FREE_PERC_AVG}`
        FROM {view_name}
        WHERE `{COUNTRY}` = "Italy"
        GROUP BY `{YEAR_MONTH}`
        ORDER BY `{YEAR_MONTH}`
    """)

    df_progress.createOrReplaceTempView(view_name_avg)

    df_classification = spark.sql(f"""
        SELECT `{YEAR_MONTH}`, `{INTENSITY_DIRECT_AVG}`, `{CARBON_FREE_PERC_AVG}` FROM (
            SELECT `{YEAR_MONTH}`, `{INTENSITY_DIRECT_AVG}`, `{CARBON_FREE_PERC_AVG}`
            FROM `{view_name_avg}`
            ORDER BY `{INTENSITY_DIRECT_AVG}` DESC LIMIT 5
        )
        UNION ALL
        SELECT `{YEAR_MONTH}`, `{INTENSITY_DIRECT_AVG}`, `{CARBON_FREE_PERC_AVG}` FROM (
            SELECT `{YEAR_MONTH}`, `{INTENSITY_DIRECT_AVG}`, `{CARBON_FREE_PERC_AVG}`
            FROM `{view_name_avg}`
            ORDER BY `{INTENSITY_DIRECT_AVG}` ASC LIMIT 5
        )
        UNION ALL
        SELECT `{YEAR_MONTH}`, `{INTENSITY_DIRECT_AVG}`, `{CARBON_FREE_PERC_AVG}` FROM (
            SELECT `{YEAR_MONTH}`, `{INTENSITY_DIRECT_AVG}`, `{CARBON_FREE_PERC_AVG}`
            FROM `{view_name_avg}`
            ORDER BY `{CARBON_FREE_PERC_AVG}` DESC LIMIT 5
        )
        UNION ALL
        SELECT `{YEAR_MONTH}`, `{INTENSITY_DIRECT_AVG}`, `{CARBON_FREE_PERC_AVG}` FROM (
            SELECT `{YEAR_MONTH}`, `{INTENSITY_DIRECT_AVG}`, `{CARBON_FREE_PERC_AVG}`
            FROM `{view_name_avg}`
            ORDER BY `{CARBON_FREE_PERC_AVG}` ASC LIMIT 5
        )
    """)

    if TIMED:
        df_progress.collect()
        df_classification.collect()
    end_time = time.time()

    #---------------------------------------------- Save results -----------------------------------------------#
    if not TIMED:
        write_results_on_hdfs(df_classification, FILE_FORMAT, result_file1)
        write_results_on_hdfs(df_progress, FILE_FORMAT, result_file2)
        write_results_on_influxdb(df_progress, "query2_sql", QUERY2_CONFIG)

    return end_time - start_time