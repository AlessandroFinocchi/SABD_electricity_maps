import argparse
import importlib


if __name__ =="__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--q",      type=int, choices=[1, 2, 3],            required=True)
    arg_parser.add_argument("--api",    type=str, choices=["rdd", "df", "sql"], required=True)
    arg_parser.add_argument("--format", type=str, choices=["csv", "parquet"],   required=True)
    arg_parser.add_argument("--cache", dest="use_cache", action="store_true", default=False)
    args = arg_parser.parse_args()

    query:int   = args.q
    api:str     = args.api
    FILE_FORMAT = args.format
    USE_CACHE   = args.use_cache

    try:
        query_module = importlib.import_module(f'query{query}.query{query}_{api}')
    except KeyError:
        raise Exception("Invalid combination of query and api.")

    query_module.run(FILE_FORMAT, USE_CACHE)