ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "dashboard"
  )

val sparkVersion = "2.4.8"
val elasticVersion = "6.8.22"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % elasticVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion
