import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.net.{URL, URLDecoder}
import scala.util.Try


object Lab02 {
  def main(args: Array[String]): Unit = {

    def decodeUrlAndGetDomain: UserDefinedFunction = udf((url: String) => {
      Try {
        new URL(URLDecoder.decode(url, "UTF-8")).getHost
      }.getOrElse("")
    })

    val spark = SparkSession.builder()
      .appName("lab02")
      .getOrCreate()

    val autousersPath = "/labs/laba02/autousers.json"
    val logs_path = "/labs/laba02/logs"

    val usersSchema =
      StructType(
        List(
          StructField("autousers", ArrayType(StringType, containsNull = false))
        )
      )

    val users = spark.read.schema(usersSchema).json(autousersPath)

    val logsSchema =
      StructType(
        List(
          StructField("uid", StringType),
          StructField("ts", StringType),
          StructField("url", StringType)
        )
      )

    val domains = spark.read.schema(logsSchema).option("delimiter", "\t").csv(logs_path)

    val users2 = users.select(explode(col("autousers")).alias("uid"))
    val domains2 = domains.withColumn("domain", decodeUrlAndGetDomain(col("url")))

    val visits = domains2
      .alias("d")
      .where(col("url").like("http%"))
      .join((users2.alias("u")), col("d.uid") === col("u.uid"), "left")
      .select(
        regexp_replace(col("domain"), "^www\\.", "").alias("url"),
        when(col("u.uid").isNull, 0).otherwise(1).alias("auto_flag")
      )
      .where(col("url") !== "")

    val total_count: Float = visits.count()
    val total_sum: Float = visits.where(col("auto_flag") === 1).count()

    def getRelevance: UserDefinedFunction = udf((sum: Int, count: Int) => {
      Math.pow((sum / total_count), 2) / ((count / total_count) * (total_sum / total_count))
    })

    val finalDF = visits
      .groupBy(col("url"))
      .agg(
        sum(col("auto_flag")).as("sum"),
        count(col("url")).as("count")
      )
      .withColumn("relevance", getRelevance(col("sum"), col("count")))
      .withColumn("relevance2", format_number(col("relevance"), 15))
      .sort(desc("relevance2"), col("url"))
      .select(col("url").alias("domain"), col("relevance2").alias("relevance"))
      .limit(200)

    val result_path = "laba02_domains.txt"
    finalDF.coalesce(1).write.mode("overwrite").option("delimiter", "\t").csv(result_path)
  }
}
