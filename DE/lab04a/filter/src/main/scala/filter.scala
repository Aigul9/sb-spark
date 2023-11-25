import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object filter {

  def main(args: Array[String]): Unit = {

    def write(df: DataFrame, path: String): Unit = {
      df
        .select("data.*", "p_date")
        .write
        .partitionBy("p_date")
        .option("quote", "")
        .mode("overwrite")
        .json(path)
    }

    val spark: SparkSession = SparkSession.builder()
      .config("spark.master", "local")
      .appName("lab04a")
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val inputTopic: String = spark.sparkContext.getConf.get("spark.filter.topic_name")
    val offset: String = spark.sparkContext.getConf.get("spark.filter.offset")
    val outputDirPrefix: String = spark.sparkContext.getConf.get("spark.filter.output_dir_prefix")

    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1.newprolab.com:6667",
      "subscribe" -> inputTopic
    )
	
    val maxOffset = 1460319

    val input = spark
      .read
      .format("kafka")
      .options(kafkaParams)
      .option("startingOffsets",
        if (offset.contains("earliest"))
          offset
        else {
          "{\"" + inputTopic + "\":{\"0\":" + (maxOffset + offset.toLong + 1) + "}}"
        }
      )
	  .option("failOnDataLoss", "false")
      .load()
      .cache()
    input.count()

    val hdfsView = s"$outputDirPrefix/view/"
    val hdfsBuy = s"$outputDirPrefix/buy/"

    val jsonSchema = new StructType()
      .add("event_type", StringType)
      .add("category", StringType)
      .add("item_id", StringType)
      .add("item_price", IntegerType)
      .add("uid", StringType)
      .add("timestamp", LongType)

    val res = input
      .select(col("value").cast("string").alias("value_string"))
      .withColumn("value_data", from_json(col("value_string"), jsonSchema))
      .withColumn("p_date", from_unixtime(col("value_data.timestamp") / 1000, "yyyyMMdd"))
      .selectExpr("struct(value_data.*, p_date as date) as data", "p_date")

    val view = res.filter(col("value_data.event_type") === "view")
    val buy = res.filter(col("value_data.event_type") === "buy")

    write(view, hdfsView)
    write(buy, hdfsBuy)

    input.unpersist()
  }
}
