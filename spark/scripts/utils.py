def year(rdd) -> int: return rdd.split(",")[0].split("-")[0]

def country(rdd) -> str: return rdd.split(",")[1]

def intensity1(rdd) -> float: return float(rdd.split(",")[4])

def free_intensity(rdd) -> float: return float(rdd.split(",")[6])

def pretty_collect(rdd):
    for result in rdd.collect():
        print(result)