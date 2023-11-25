ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "data_mart"
  )

val sparkVersion = "2.4.8"
val cassandraVersion = "2.4.3"
val elasticVersion = "6.8.22"
val postgresVersion = "42.3.3"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % cassandraVersion
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % elasticVersion
libraryDependencies += "org.postgresql" % "postgresql" % postgresVersion
