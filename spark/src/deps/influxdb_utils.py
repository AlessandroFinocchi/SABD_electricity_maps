from datetime import datetime, timezone, timedelta
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, WriteApi
from deps.config import INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET, InfluxWriterConfig
from pyspark.sql.dataframe import DataFrame


def get_write_api():
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    return client, client.write_api(write_options=SYNCHRONOUS)

def write_results_on_influxdb(df_res: DataFrame,
                              measurement: str,
                              config: InfluxWriterConfig
                             ):
    """
        writes the dataframe results to InfluxDB.

        :param df_res: df to write.
        :param measurement: measurement name
        :param config: info about the columns to write.
    """
    client, write_api = get_write_api()

    header = config.header
    time_format = config.time_format
    timestamp_col = config.timestamp_col
    tag_cols = config.tag_cols
    field_cols = config.field_cols

    for row in df_res.collect():
        timestamp = datetime.strptime(str(row[timestamp_col]), time_format).replace(tzinfo=timezone.utc)
        point = Point(measurement).time(timestamp, WritePrecision.S)

        for i in tag_cols:   point.tag(header[i], row[i])
        for i in field_cols: point.field(header[i], row[i])

        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

    client.close()


def write_job_time_on_influxdb(write_api: WriteApi,
                               measurement: str,
                               job_time: float,
                               run_num: int):
    timestamp = datetime.now()
    point = Point(measurement) \
            .field("job_time", job_time) \
            .field("run_num", run_num) \
            .time(timestamp, WritePrecision.S)

    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)