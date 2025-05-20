import os
import requests

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

if __name__ == '__main__':
    hdfs_base = 'hdfs://namenode:54310'
    spark = SparkSession.builder\
        .appName('scraper')\
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    url = 'https://data.electricitymaps.com/2025-04-03/{}_{}_hourly.csv'
    countries = ['IT', 'SE']
    years = [2021 + i for i in range(0, 4)]

    for country in countries:
        for year in years:
            query = url.format(country, year)
            response = requests.get(query)
            path = f'/opt/spark/scripts/{country}_{year}.csv'

            with open(path, 'w') as fp:
                if response.status_code != 200:
                    print(f'error code {response.status_code} with request {query}')
                    continue
                fp.write(response.text)
                os.chmod(path, 0o777)

            df = spark.read.csv(path, header=True, inferSchema=True)
            df.write.mode("overwrite").csv(f'{hdfs_base}/data/{country}_{year}.csv', header=True)
            df.write.mode("overwrite").parquet(f'{hdfs_base}/data/{country}_{year}.parquet')
            os.remove(path)

    for country in countries:
        aggregate_df = None

        for year in years:
            path = f'{hdfs_base}/data/{country}_{year}.csv'
            df = spark.read.csv(path, header=True, inferSchema=True).withColumn('year', lit(year))
            aggregate_df = df if aggregate_df is None else aggregate_df.unionByName(df)

        # aggregate_df.coalesce(1).write.mode("overwrite").parquet("/path/to/output_folder")
        aggregate_df.write.mode("overwrite").csv(f'{hdfs_base}/data/{country}_all.csv', header=True)
        aggregate_df.write.mode("overwrite").parquet(f'{hdfs_base}/data/{country}_all.parquet')

            
