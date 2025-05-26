from pyspark.sql import SparkSession
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
result_file = f"hdfs://namenode:54310/data/results/query1_rdd.{FILE_FORMAT}"
while not exists_on_hdfs(it_file, sc) or not exists_on_hdfs(se_file, sc):
    nr.run_nifi_flow()
    time.sleep(1)

#--------------------------------------------- Process results ---------------------------------------------#
it_rdd = sc.textFile(it_file)
se_rdd = sc.textFile(se_file)

rdd = it_rdd.union(se_rdd)
rdd_map = rdd.map(lambda x: ((country(x), year(x)), (intensity1(x), free_intensity(x), 1)))
rdd_map = rdd_map.cache() if USE_CACHE else rdd_map

rdd_extremes = rdd_map.reduceByKey(lambda x, y: (
    min(x[0], y[0]),
    min(x[1], y[1]),
    max(x[0], y[0]),
    max(x[1], y[1])
)).sortByKey()

rdd_min = rdd_map.reduceByKey(lambda x, y: (min(x[0], y[0]), min(x[1], y[1])))
rdd_max = rdd_map.reduceByKey(lambda x, y: (max(x[0], y[0]), max(x[1], y[1])))
rdd_avg = rdd_map.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])) \
                 .map(lambda x: (x[0], (x[1][0] / x[1][2], x[1][1] / x[1][2])))

rdd_results = rdd_min.join(rdd_avg) \
                     .join(rdd_max) \
                     .sortByKey() \
                     .map(lambda x:(
                                    x[0][0],        # country
                                    x[0][1],        # year
                                    x[1][0][1][0],  # avg carbon
                                    x[1][0][0][0],  # min carbon
                                    x[1][1][0],     # max carbon
                                    x[1][0][1][1],  # avg cfe
                                    x[1][0][0][1],  # min cfe
                                    x[1][1][1]      # max cfe
                                    )
                          )

#---------------------------------------------- Save results -----------------------------------------------#
# rdd_results.coalesce(1) \
#            .saveAsTextFile(result_file)

df_res = rdd_results.toDF(QUERY1_HEADER)
df_res.show()
store_results_on_hdfs(df_res, FILE_FORMAT, result_file)

spark.stop()
print("Results saved successfully.")