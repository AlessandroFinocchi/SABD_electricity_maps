import argparse
import importlib

from deps.influxdb_utils import write_job_time_on_influxdb, get_write_api

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--q",      type=int, choices=[1, 2, 3],            required=True)
    arg_parser.add_argument("--api",    type=str, choices=["rdd", "df", "sql"], required=True)
    arg_parser.add_argument("--format", type=str, choices=["csv", "parquet"],   required=True)
    arg_parser.add_argument("--times",  type=int, default=5,                  required=False)
    arg_parser.add_argument("--cache", dest="use_cache", action="store_true", default=False)
    args = arg_parser.parse_args()

    query:int   = args.q
    api:str     = args.api
    times:int   = args.times
    FILE_FORMAT = args.format
    USE_CACHE   = args.use_cache

    if USE_CACHE and api != "rdd": raise Exception("Cache is not supported for query 1 or 2 with DF or SQL API.")

    try:
        query_module = importlib.import_module(f'query{query}.query{query}_{api}')
    except KeyError:
        raise Exception("Invalid combination of query and api.")

    client, write_api = get_write_api()
    time_sum = 0
    for i in range(1, times+1):
        time: float = query_module.run(FILE_FORMAT, USE_CACHE, TIMED=True)
        time_sum += time
        remaining_seconds = time_sum / (i+1) * (times-i)
        secs  = remaining_seconds % 60
        mins  = (remaining_seconds % 3600) // 60
        hours = remaining_seconds // 3600
        print(f"Query {query} - {api} - {FILE_FORMAT} - Run {i} - Time: {time} s")
        print(f"Expected remaining time: {hours} h {mins} m {secs} s")
        write_job_time_on_influxdb(write_api=write_api,
                                   measurement=f"perf_query{query}_{api}_{FILE_FORMAT}",
                                   job_time=time,
                                   run_num=i,
                                   use_cache = USE_CACHE)

    client.close()