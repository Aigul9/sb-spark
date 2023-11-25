package com.newprolab

import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import java.io._
import scala.io.BufferedSource
import scala.io.Source.fromFile

object Lab01 {

  def main(args: Array[String]): Unit = {
    val source: BufferedSource = fromFile("data/u.data")

    val uData = source.getLines
      .toList
      .map(string => string.split("\t"))
      .map(x => (x(2), x(1)))

    source.close()

    val filmList = uData
      .filter(_._2 == "302")
      .groupBy(_._1)
      .mapValues(_.size)
      .toList
      .sortBy(_._1)
      .map(_._2)

    val filmsList = uData
      .groupBy(_._1)
      .mapValues(_.size)
      .toList
      .sortBy(_._1)
      .map(_._2)

    val jsonData = Map("hist_film" -> filmList, "hist_all" -> filmsList)

    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    val jsonString = Serialization.write(jsonData)

    val file: File = new File("lab01.json")

    val writer: BufferedWriter = new BufferedWriter(new FileWriter(file))
    writer.write(jsonString)
    writer.close()

  }
}
