from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Streaming Word Count").config("spark.streaming.stopGracefullyOnShutdown", "true").config("spark.sql.shuffle.partitions", 3).getOrCreate()

    lines_df = spark.readStream.format("socket").option("host", "localhost").option("port", "9999").load()

    #lines_df.printSchema()

    words_df = lines_df.select(expr("explode(split(value,' ')) as word"))
    counts_df = words_df.groupBy("word").count()
    word_count_query = counts_df.writeStream.format("console").option("checkpointLocation", "chk-point-dir").outputMode("complete").start()

    word_count_query.awaitTermination()
