from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("HDFS File Row Count") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:54310") \
        .getOrCreate()

    # Read the text file from HDFS
    df = spark.read.text("hdfs://namenode:54310/data/test_file.txt")

    df.write.text("hdfs://namenode:54310/data/results.txt")

    # Count the number of rows
    row_count = df.count()

    print(f"Number of rows in test_file.txt: {row_count}")

