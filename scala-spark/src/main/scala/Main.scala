import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._

object Main {
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.WARN)

  def main(args:Array[String]) {
    
    
  // Define the Spark session
  val spark = SparkSession.builder
    .appName("WebSocketWordCount")
    .master("local[*]")  // Set to local mode for testing
    .getOrCreate()

  import spark.implicits._

  // WebSocket source (custom connection might be needed)
  // WebSocket data could be streamed using a Kafka topic for structured streaming compatibility

  val socketDF = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)  // Port of your WebSocket server
    .load()

  // Split the input data (lines) into words
  val words = socketDF
    .as[String]
    .flatMap(_.split(" ")).alias("value")

  // Perform word count on the words stream
  val wordCounts = words.groupBy("value").count()

  // Start the streaming query and output to console
  val query = wordCounts.writeStream
    .outputMode("complete") // Use complete mode for full counts
    .format("console")      // Print to console
    .start()

  query.awaitTermination()  // Keep the stream running
  }
}
