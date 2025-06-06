import argparse
import importlib

from deps.utils import get_spark, check_hdfs


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
    check_hdfs(sc, energy_file)

    #----------------------------------------------- Execute job -----------------------------------------------#
    _ = query_module.run(spark, sc, energy_file, FILE_FORMAT, TIMED=False)
    spark.stop()