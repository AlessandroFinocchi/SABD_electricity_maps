from enum import Enum

class Formats(Enum):
    CSV     = "csv"
    PARQUET = "parquet"

class Headers(Enum):
    DATE                 = "Datetime (UTC)",
    COUNTRY              = "Country",
    ZONE_NAME            = "Zone name",
    ZONE_ID              = "Zone id",
    INTENSITY_DIRECT     = "Carbon intensity gCO₂eq/kWh (direct)",
    INTENSITY_CYCLE      = "Carbon intensity gCO₂eq/kWh (Life cycle)",
    CARBON_FREE_PERC     = "Carbon-free energy percentage (CFE%)",
    RENEWABLE_PERC       = "Renewable energy percentage (RE%)",
    DATA_SOURCE          = "Data source",
    DATA_ESTIMATED       = "Data estimated",
    DATA_ESTIMATION_METH = "Data estimation method"


USE_CACHE = False
FORMAT = Formats.CSV.value