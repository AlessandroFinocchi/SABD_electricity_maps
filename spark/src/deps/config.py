CSV     = "csv"
PARQUET = "parquet"
ORIGINAL_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"
QUERY1_DATE_FORMAT   = "%Y"
QUERY2_DATE_FORMAT   = "%Y_%m"

# Original headers
DATE                 = "Datetime"
COUNTRY              = "Country"
ZONE_NAME            = "Zone_name"
ZONE_ID              = "Zone_id"
INTENSITY_DIRECT     = "CO2_int_direct"
INTENSITY_CYCLE      = "CO2_int_lc"
CARBON_FREE_PERC     = "CFE_%"
RENEWABLE_PERC       = "RE_%"
DATA_SOURCE          = "Data_source"
DATA_ESTIMATED       = "Data_estimated"
DATA_ESTIMATION_METH = "Data_estimation method"

# New headers
YEAR                 = "Year"
MONTH                = "Month"
YEAR_MONTH           = f"{YEAR}_{MONTH}"
INTENSITY_DIRECT_MIN = "CO2_int_min"
INTENSITY_DIRECT_AVG = "CO2_int_avg"
INTENSITY_DIRECT_MAX = "CO2_int_max"
CARBON_FREE_PERC_MIN = "CFE_%_min"
CARBON_FREE_PERC_AVG = "CFE_%_avg"
CARBON_FREE_PERC_MAX = "CFE_%_max"

# Headers
ORIGINAL_HEADER = [DATE,
          COUNTRY,
          ZONE_NAME,
          ZONE_ID,
          INTENSITY_DIRECT,
          INTENSITY_CYCLE,
          CARBON_FREE_PERC,
          RENEWABLE_PERC,
          DATA_SOURCE,
          DATA_ESTIMATED,
          DATA_ESTIMATION_METH]

QUERY1_HEADER = [YEAR,
                 COUNTRY,
                 INTENSITY_DIRECT_AVG,
                 INTENSITY_DIRECT_MIN,
                 INTENSITY_DIRECT_MAX,
                 CARBON_FREE_PERC_AVG,
                 CARBON_FREE_PERC_MIN,
                 CARBON_FREE_PERC_MAX]

QUERY2_HEADER = [YEAR_MONTH,
                 INTENSITY_DIRECT_AVG,
                 CARBON_FREE_PERC_AVG]

# Influx reference constants
INFLUXDB_URL="http://influxdb2:8086"
INFLUXDB_ORG="it.uniroma2"
INFLUXDB_BUCKET="query_results"
with open("/opt/spark/code/src/env/influxdb2-admin-token", 'r') as file:
    INFLUXDB_TOKEN = file.read()

class InfluxWriterConfig:
    def __init__(self, header,
                 time_format: str,
                 timestamp_col: int,
                 tag_cols,
                 field_cols):
        """
        :param header:
        :type header: list[str]
        :param time_format:
        :param timestamp_col:
        :param tag_cols:
        :type tag_cols: list[int]
        :param field_cols:
        :type field_cols: list[int]
        """
        self.header = header
        self.time_format = time_format
        self.timestamp_col = timestamp_col
        self.tag_cols = tag_cols
        self.field_cols = field_cols

QUERY1_CONFIG = InfluxWriterConfig(header=QUERY1_HEADER,
                                   time_format=QUERY1_DATE_FORMAT,
                                   timestamp_col=0,
                                   tag_cols=[1],
                                   field_cols=[2, 3, 4, 5, 6, 7])

QUERY2_CONFIG = InfluxWriterConfig(header=QUERY2_HEADER,
                                   time_format=QUERY2_DATE_FORMAT,
                                   timestamp_col=0,
                                   tag_cols=[],
                                   field_cols=[1, 2]
)