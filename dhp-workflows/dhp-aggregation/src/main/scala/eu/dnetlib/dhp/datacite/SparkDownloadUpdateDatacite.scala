package eu.dnetlib.dhp.datacite

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.{Oaf, Result}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.text.SimpleDateFormat
import java.util.Locale
import scala.io.Source

object SparkDownloadUpdateDatacite {
  val log: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    val parser = new ArgumentApplicationParser(
      Source
        .fromInputStream(
          getClass.getResourceAsStream("/eu/dnetlib/dhp/datacite/generate_dataset_params.json")
        )
        .mkString
    )
    parser.parseArgument(args)
    val master = parser.get("master")
    val sourcePath = parser.get("sourcePath")
    val workingPath = parser.get("workingPath")

    val hdfsuri = parser.get("namenode")
    log.info(s"namenode is $hdfsuri")

    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .appName(getClass.getSimpleName)
      .master(master)
      .getOrCreate()

    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]
    implicit val resEncoder: Encoder[Result] = Encoders.kryo[Result]

    import spark.implicits._

    val maxDate: String = spark.read
      .load(workingPath)
      .as[Oaf]
      .filter(s => s.isInstanceOf[Result])
      .map(r => r.asInstanceOf[Result].getDateofcollection)
      .select(max("value"))
      .first()
      .getString(0)
    val ISO8601FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ", Locale.US)
    val string_to_date = ISO8601FORMAT.parse(maxDate)
    val ts = string_to_date.getTime

  }

}
