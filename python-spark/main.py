from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from pyspark.sql.types import *
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("socket-stream").getOrCreate()

lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

words=lines.select(explode(split(lines.value, " ")).alias("word"))

## process incoming word count
counts = words.groupBy("word").count()

checkpoint_dir = "./checkpoint/"

query = counts.writeStream.outputMode("complete").format("console").trigger(processingTime="5 seconds").option("checkpointLocation", checkpoint_dir).start()

query.awaitTermination()