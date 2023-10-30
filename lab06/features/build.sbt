ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "features"
  )

val sparkVersion = "2.4.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
