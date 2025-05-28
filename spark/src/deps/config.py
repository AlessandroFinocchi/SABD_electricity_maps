CSV     = "csv"
PARQUET = "parquet"
DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"

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
INTENSITY_DIRECT_MIN = "CO2_int_direct_min"
INTENSITY_DIRECT_AVG = "CO2_int_direct_avg"
INTENSITY_DIRECT_MAX = "CO2_int_direct_max"
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

# Influx
INFLUXDB_URL="http://influxdb2:8086"
INFLUXDB_TOKEN="my-token"
INFLUXDB_ORG="it.uniroma2"
INFLUXDB_BUCKET="query_results"