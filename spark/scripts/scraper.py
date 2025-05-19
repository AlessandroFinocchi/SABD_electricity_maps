import os
import requests

from pyspark.sql import SparkSession

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
            os.remove(path)
