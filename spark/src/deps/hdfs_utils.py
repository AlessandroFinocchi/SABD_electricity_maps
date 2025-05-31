from pyspark import SparkContext
from deps.config import CSV, PARQUET


def exists_on_hdfs(path_str:str, sc: SparkContext) -> bool:
    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    path = sc._jvm.org.apache.hadoop.fs.Path(path_str)
    if fs.exists(path):
        print(f"OK: Input file '{path_str}' found in HDFS.")
        return True
    else:
        print(f"ERROR: Input file '{path_str}' not found in HDFS.")
        return False

def write_results_on_hdfs(result, file_format:str, result_file:str):
    # result.show()
    if file_format == CSV: result.coalesce(1).write.mode("overwrite").csv(result_file)
    elif file_format == PARQUET: result.coalesce(1).write.mode("overwrite").parquet(result_file)
    else: raise Exception(f"Unsupported file format: {file_format}")
    print(f"Results {result_file} saved successfully.")