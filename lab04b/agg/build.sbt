ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "agg"
  )

val sparkVersion = "2.4.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
