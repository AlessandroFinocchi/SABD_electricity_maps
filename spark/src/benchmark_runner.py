import argparse
import importlib

from deps.config import SEP
from deps.influxdb_utils import write_job_time_on_influxdb, get_write_api
from deps.utils import get_spark, check_hdfs


def print_logs(run_time: float, num_run: int, times: int):
    if not hasattr(print_logs, "time_sum"):
        print_logs.time_sum = 0
    print_logs.time_sum += run_time

    run_width = len(str(times))
    remaining_seconds = print_logs.time_sum / (num_run+1) * (times - num_run)
    secs  = remaining_seconds % 60
    mins  = int((remaining_seconds % 3600) // 60)
    hours = int(remaining_seconds // 3600)

    print(f"Run {num_run:>{run_width}d} - Time elapsed: {run_time:7.4f} s - Expected remaining time: {hours:2d} h {mins:2d} m {secs:5.2f} s")

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--q",      type=int, choices=[1, 2, 3],            required=True)
    arg_parser.add_argument("--api",    type=str, choices=["rdd", "df", "sql"], required=True)
    arg_parser.add_argument("--format", type=str, choices=["csv", "parquet"],   required=True)
    arg_parser.add_argument("--times",  type=int, default=100,                  required=False)
    arg_parser.add_argument("--log ",   dest="use_logs",  action="store_true",  default=True)
    args = arg_parser.parse_args()

    QUERY:int   = args.q
    API:str     = args.api
    TIMES:int   = args.times
    FILE_FORMAT = args.format
    LOGS        = args.use_logs

    #----------------------------------------------- Check hdfs ------------------------------------------------#
    spark, sc = get_spark(f"Query {QUERY} - {API}")
    energy_file = f"hdfs://namenode:54310/data/country_all.{FILE_FORMAT}"
    check_hdfs(sc, energy_file)
    spark.stop()

    #----------------------------------------------- Execute job ------------------------------------------------#
    try: query_module = importlib.import_module(f'query{QUERY}.query{QUERY}_{API}')
    except KeyError: raise Exception("Invalid combination of query and api.")

    client, write_api = get_write_api()
    time_sum = 0

    print(f"{SEP} Query {QUERY} {API} {FILE_FORMAT} {SEP}")
    for num_run in range(1, TIMES + 1):
        spark, sc = get_spark(f"Query {QUERY} - {API}")
        time: float = query_module.run(spark, sc, energy_file, FILE_FORMAT, TIMED=True)
        spark.stop()

        if LOGS: print_logs(time, num_run, TIMES)

        write_job_time_on_influxdb(write_api=write_api,
                                   measurement=f"perf_query{QUERY}_{API}_{FILE_FORMAT}",
                                   job_time=time,
                                   run_num=num_run)

    client.close()