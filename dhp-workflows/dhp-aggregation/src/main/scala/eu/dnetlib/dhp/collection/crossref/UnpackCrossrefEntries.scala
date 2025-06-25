/*
 * Copyright (c) 2025.
 * SPDX-FileCopyrightText: © 2023 Consiglio Nazionale delle Ricerche
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package eu.dnetlib.dhp.collection.crossref

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

object UnpackCrossrefEntries {

  val log: Logger = LoggerFactory.getLogger(UnpackCrossrefEntries.getClass)

  def extractDump(input: String): List[String] = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)

    val a = (json \ "items").extract[JArray]
    a.arr.map(s => compact(render(s)))

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    val parser = new ArgumentApplicationParser(
      Source
        .fromInputStream(
          getClass.getResourceAsStream(
            "/eu/dnetlib/dhp/collection/crossref_dump_reader/generate_dataset_params.json"
          )
        )
        .mkString
    )
    parser.parseArgument(args)
    val master = parser.get("master")
    val sourcePath = parser.get("sourcePath")
    val targetPath = parser.get("targetPath")

    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .appName(UnpackCrossrefEntries.getClass.getSimpleName)
      .master(master)
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    sc.wholeTextFiles(sourcePath, 6000)
      .flatMap(d => extractDump(d._2))
      .saveAsTextFile(targetPath, classOf[GzipCodec])

  }

}
