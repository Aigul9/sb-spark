import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.PipelineModel


object dashboard {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .config("spark.sql.session.timeZone", "UTC")
      .appName("lab08")
      .getOrCreate()

    val lab07Path = "/labs/laba08"

    val testInput = spark.read.json(lab07Path)

    val modelSaved = PipelineModel.load("lab07/model")

    val test = testInput
      .withColumn("visit", explode(col("visits")))
      .withColumn("host", lower(callUDF("parse_url", col("visit.url"), lit("HOST"))))
      .withColumn("domain", regexp_replace(col("host"), "www.", ""))
      .groupBy("uid", "date")
      .agg(collect_list("domain").alias("domains"))

    val predictions = modelSaved
      .transform(test)
      .select(col("uid"), col("gender_age_prediction").alias("gender_age"), col("date"))

    val esConfig = Map(
      "es.nodes" -> "10.0.0.31",
      "es.port" -> "9200",
      "es.index.auto.create" -> "false",
      "es.write.operation" -> "index",
      "es.mapping.id" -> "uid",
      "es.read.metadata" -> "true",
      "es.nodes.wan.only" -> "true",
      "es.net.ssl" -> "false"
    )

    predictions
      .write
      .format("org.elasticsearch.spark.sql")
      .options(esConfig)
      .mode("overwrite")
      .save("aigul_sibgatullina_lab08/_doc")

  }
}
