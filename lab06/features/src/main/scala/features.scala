import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object features {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .config("spark.sql.session.timeZone", "UTC")
      .appName("lab06")
      .getOrCreate()

    val weblogsPath = "/labs/laba03/weblogs.json"
    val usersItemsPath = "/user/aigul.sibgatullina/users-items/20200429"

    val weblogsSchema = StructType(
      List(
        StructField("uid", StringType),
        StructField("visits", StringType)
      )
    )

    val weblogs = spark.read.schema(weblogsSchema).json(weblogsPath).cache()
    val usersItems = spark.read.option("inferSchema", "true").parquet(usersItemsPath)

    weblogs.count()

    val visitSchema = ArrayType(MapType(StringType, StringType))

    val weblogsWithDomain = weblogs
      .withColumn("visit", explode(from_json(col("visits"), visitSchema)))
      .withColumn("host", lower(callUDF("parse_url", col("visit.url"), lit("HOST"))))
      .withColumn("domain", regexp_replace(col("host"), "www.", ""))

    val domainsTop1000 = weblogsWithDomain
      .where(col("domain").isNotNull)
      .groupBy("domain")
      .count()
      .sort(desc("count"))
      .select("domain")
      .limit(1000)

    val usersPivoted = weblogsWithDomain
      .groupBy("uid", "domain")
      .count()
      .join(domainsTop1000, Seq("domain"), "right")
      .groupBy("uid")
      .pivot("domain")
      .sum("count")
      .na.fill(0)

    val columnNames = usersPivoted.columns.filter(_ != "uid").sorted

    val usersWithDomainFeatures = columnNames
      .foldLeft(usersPivoted) { (df, columnName) =>
        df.withColumn(columnName, col(s"`$columnName`").cast("int"))
      }
      .select(
        col("uid"),
        array(usersPivoted.columns.filter(_ != "uid").map(c => col("`" + c + "`")): _*).alias("domain_features")
      )

    val weblogsWithDomainWithDayAndHour = weblogsWithDomain
      .select(col("uid"), from_unixtime(col("visit.timestamp") / 1000).alias("date"), col("domain"))
      .withColumn("day", date_format(col("date"), "E"))
      .withColumn("hour", hour(col("date")))

    val daysOfWeek = Seq("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
    val weblogsWithDomainWithDayPartTemp = weblogsWithDomainWithDayAndHour
      .groupBy("uid")
      .pivot("day", daysOfWeek)
      .count()
      .na.fill(0)
    val weblogsWithDomainWithDayPart = daysOfWeek.foldLeft(weblogsWithDomainWithDayPartTemp) { (df, day) =>
      df.withColumnRenamed(day, s"web_day_${day.toLowerCase}")
    }

    val hoursOfDay = (0 to 23).map(_.toString)
    val weblogsWithDomainWithHourTemp = weblogsWithDomainWithDayAndHour
      .groupBy("uid")
      .pivot("hour", hoursOfDay)
      .count()
      .na.fill(0)
    val weblogsWithDomainWithHour = hoursOfDay.foldLeft(weblogsWithDomainWithHourTemp) { (df, hour) =>
      df.withColumnRenamed(hour, s"web_hour_$hour")
    }

    val weblogsWithDomainWithFractions = weblogsWithDomainWithDayAndHour
      .groupBy("uid")
      .agg(
        sum(when(col("hour").between(9, 17), 1).otherwise(0)).alias("work_hours_visits"),
        sum(when(col("hour").between(18, 23), 1).otherwise(0)).alias("evening_hours_visits"),
        count("uid").alias("total_user_visits")
      )
      .select(
        col("uid"),
        (col("work_hours_visits") / col("total_user_visits")).alias("web_fraction_work_hours"),
        (col("evening_hours_visits") / col("total_user_visits")).alias("web_fraction_evening_hours")
      )

    val finalDF = weblogsWithDomainWithDayPart
      .join(weblogsWithDomainWithHour, Seq("uid"), "inner")
      .join(weblogsWithDomainWithFractions, Seq("uid"), "inner")
      .join(usersWithDomainFeatures, Seq("uid"), "inner")
      .join(usersItems, Seq("uid"), "inner")

    val outputPath = "/user/aigul.sibgatullina/features"
    finalDF
      .write
      .mode("overwrite")
      .parquet(outputPath)

    weblogs.unpersist()
  }
}
