import Config._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.postgresql.Driver

import java.net.{URL, URLDecoder}
import java.sql.{Connection, DriverManager, Statement}
import scala.util.Try


object Config {

  val host: String = "10.0.0.31"
  val postgresPort: String = "5432"
  val cassandraPort: String = "9042"
  val elasticPort: String = "9200"
  val hdfsPath: String = "hdfs:///labs/laba03/weblogs.json"
  val keyspace: String = "labdata"
  val postgresTable: String = "domain_cats"
  val elasticTable: String = "visits"
  val user: String = "aigul_sibgatullina"
  val password: String = "PhnswCPX"
  val table: String = "clients"
  val checker: String = "labchecker2"
}


object data_mart {

  private def decodeUrlAndGetDomain: UserDefinedFunction = udf((url: String) => {
    Try {
      new URL(URLDecoder.decode(url, "UTF-8")).getHost
    }.getOrElse("")
  })

  private def grantTable(): Unit = {
    val driverClass: Class[Driver] = classOf[org.postgresql.Driver]
    val driver: Any = Class.forName("org.postgresql.Driver").newInstance()
    val url = s"jdbc:postgresql://$host:$postgresPort/$user?user=$user&password=$password"
    val connection: Connection = DriverManager.getConnection(url)
    val statement: Statement = connection.createStatement()
    val bool: Boolean = statement.execute(s"GRANT SELECT ON $table TO $checker")
    connection.close()
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .config("spark.cassandra.connection.host", host)
      .config("spark.cassandra.connection.port", cassandraPort)
      .config("spark.master", "local")
      .appName(s"$user\\_lab03")
      .getOrCreate()

    val clients: DataFrame = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> table, "keyspace" -> keyspace))
      .load()

    val visits: DataFrame = spark.read
      .format("org.elasticsearch.spark.sql")
      .options(Map("es.read.metadata" -> "true",
        "es.nodes.wan.only" -> "true",
        "es.port" -> elasticPort,
        "es.nodes" -> host,
        "es.net.ssl" -> "false"))
      .load(elasticTable)

    val logs: DataFrame = spark.read
      .json(hdfsPath)

    val cats: DataFrame = spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://$host:$postgresPort/$keyspace")
      .option("dbtable", postgresTable)
      .option("user", user)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .load()

    val shopsPivot = clients.
      join(visits, Seq("uid"), "inner")
      .groupBy("uid", "gender", "age")
      .pivot("category")
      .agg(count(col("uid")))

    val excludedColumns = Set("uid", "gender", "age")

    var shops = shopsPivot

    for (colName <- shopsPivot.columns) {
      if (!excludedColumns.contains(colName)) {
        shops = shops.withColumnRenamed(colName, "shop_" + colName.toLowerCase)
      }
    }

    val webPivot = clients.
      join(logs, Seq("uid"), "inner")
      .withColumn("ex", explode(col("visits")))
      .withColumn("url", col("ex").getItem("url"))
      .withColumn("domain", regexp_replace(decodeUrlAndGetDomain(col("url")), "^www\\.", ""))
      .join(cats, Seq("domain"), "inner")
      .groupBy("uid", "gender", "age")
      .pivot("category")
      .agg(count(col("uid")))

    var webs = webPivot

    for (colName <- webPivot.columns) {
      if (!excludedColumns.contains(colName)) {
        webs = webs.withColumnRenamed(colName, "web_" + colName.toLowerCase)
      }
    }

    val shopColumns = shops.columns.filter(_.startsWith("shop_")).map(col)
    val webColumns = webs.columns.filter(_.startsWith("web_")).map(col)

    val finalDF = shops.alias("shops")
      .join(webs.alias("webs"), col("shops.uid") === col("webs.uid"), "outer")
      .withColumn("uid_joined", coalesce(col("shops.uid"), col("webs.uid")))
      .withColumn("gender_joined", coalesce(col("shops.gender"), col("webs.gender")))
      .withColumn("age_joined", coalesce(col("shops.age"), col("webs.age")))
      .withColumn(
        "age_cat",
        when(col("age_joined").between(18, 24), "18-24")
          .when(col("age_joined").between(25, 34), "25-34")
          .when(col("age_joined").between(35, 44), "35-44")
          .when(col("age_joined").between(45, 54), "45-54")
          .otherwise(">=55")
      )
      .select(Seq(col("uid_joined").alias("uid"), col("gender_joined").alias("gender"), col("age_cat")) ++ shopColumns ++ webColumns: _*)

    finalDF.write
      .format("jdbc")
      .option("url", s"jdbc:postgresql://$host:$postgresPort/$user")
      .option("dbtable", table)
      .option("user", user)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")
      .option("truncate", value = true)
      .mode("overwrite")
      .save()

    grantTable()

    spark.stop()
  }
}
