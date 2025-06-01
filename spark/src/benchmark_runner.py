import argparse
import importlib

from deps.influxdb_utils import write_job_time_on_influxdb, get_write_api
from deps.utils import get_spark

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--q",      type=int, choices=[1, 2, 3],            required=True)
    arg_parser.add_argument("--api",    type=str, choices=["rdd", "df", "sql"], required=True)
    arg_parser.add_argument("--format", type=str, choices=["csv", "parquet"],   required=True)
    arg_parser.add_argument("--times",  type=int, default=50,                  required=False)
    arg_parser.add_argument("--cache", dest="use_cache", action="store_true", default=False)
    arg_parser.add_argument("--log ",  dest="use_logs",  action="store_true", default=False)
    args = arg_parser.parse_args()

    QUERY:int   = args.q
    API:str     = args.api
    TIMES:int   = args.times
    FILE_FORMAT = args.format
    USE_CACHE   = args.use_cache
    LOGS        = args.use_logs

    if USE_CACHE and API != "rdd": raise Exception("Cache is not supported for query 1 or 2 with DF or SQL API.")

    try: query_module = importlib.import_module(f'query{QUERY}.query{QUERY}_{API}')
    except KeyError: raise Exception("Invalid combination of query and api.")

    client, write_api = get_write_api()
    time_sum = 0
    for num_run in range(1, TIMES + 1):
        spark, sc = get_spark(f"Query {QUERY} - {API}")
        time: float = query_module.run(spark, sc, FILE_FORMAT, USE_CACHE, TIMED=True)
        spark.stop()

        if LOGS: print_logs(time, num_run, TIMES)

        write_job_time_on_influxdb(write_api=write_api,
                                   measurement=f"perf_query{QUERY}_{API}_{FILE_FORMAT}",
                                   job_time=time,
                                   run_num=num_run,
                                   use_cache = USE_CACHE)

    client.close()


def print_logs(run_time: float, num_run: int, times: int):
    if not hasattr(print_logs, "time_sum"):
        print_logs.total = 0
    print_logs.time_sum += run_time

    remaining_seconds = print_logs.time_sum / (num_run+1) * (times - num_run)

    secs  = remaining_seconds % 60
    mins  = (remaining_seconds % 3600) // 60
    hours = remaining_seconds // 3600
    print(f"Run {num_run} - Time: {run_time} s")
    print(f"Expected remaining time: {hours} h {mins} m {secs} s")