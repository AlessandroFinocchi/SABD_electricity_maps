import argparse
import importlib
import time

from urllib3.exceptions import NewConnectionError, MaxRetryError

from deps.hdfs_utils import exists_on_hdfs
from deps.utils import get_spark
from deps import nifi_utils as nr


if __name__ =="__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--q",      type=int, choices=[1, 2, 3],            required=True)
    arg_parser.add_argument("--api",    type=str, choices=["rdd", "df", "sql"], required=True)
    arg_parser.add_argument("--format", type=str, choices=["csv", "parquet"],   required=True)
    args = arg_parser.parse_args()

    QUERY:int   = args.q
    API:str     = args.api
    FILE_FORMAT = args.format

    try: query_module = importlib.import_module(f'query{QUERY}.query{QUERY}_{API}')
    except KeyError: raise Exception("Invalid combination of query and api.")

    spark, sc = get_spark(f"Query {QUERY} - {API}")
    #----------------------------------------------- Check hdfs ------------------------------------------------#
    energy_file = f"hdfs://namenode:54310/data/country_all.{FILE_FORMAT}"
    while not exists_on_hdfs(energy_file, sc):
        try:
            nr.run_nifi_flow()
            time.sleep(5)
        except Exception as e:
            print(f"NiFi not up yet, waiting 5s…")
            time.sleep(5)
    nr.stop_nifi_flow()

    #----------------------------------------------- Execute job -----------------------------------------------#
    _ = query_module.run(spark, sc, energy_file, FILE_FORMAT, TIMED=False)
    spark.stop()