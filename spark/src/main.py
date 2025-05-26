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

    MODULE_MAP = {
        (1, "rdd"): "query1.query1_rdd",
        (1, "df"):  "query1.query1_df",
        (1, "sql"): "query1.query1_sql",
    }
    try:
        mod_name = MODULE_MAP[(query, api)]
        query_mod = importlib.import_module(mod_name)
    except KeyError:
        raise Exception("Invalid combination of query and api.")

    query_mod.run(FILE_FORMAT, USE_CACHE)
