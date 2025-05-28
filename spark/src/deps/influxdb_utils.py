from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteApi
from config import INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_BUCKET, INFLUXDB_ORG

def __write_row(row, write_api: WriteApi):
    metric, value, location = row
    point = Point(metric) \
        .tag("location", location) \
        .field("value", float(value)) \
        .time(None, WritePrecision.NS)
    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

def write_results_on_influxdb(df_res):
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = client.write_api()

    for row in df_res.collect():
        __write_row(row, write_api)

    client.close()


