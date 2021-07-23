package eu.dnetlib.dhp.sx.graph
import com.cloudera.com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.{OtherResearchProduct, Publication, Result, Software, Dataset => OafDataset}
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
object SparkConvertRDDtoDataset {

  def main(args: Array[String]): Unit = {
    val entities = List(
      ("dataset", classOf[OafDataset]),
      ("otherresearchproduct", classOf[OtherResearchProduct]),
      ("publication", classOf[Publication]),
      ("software", classOf[Software])
    )

    val log: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/convert_dataset_json_params.json")))
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()

    val sourcePath = parser.get("sourcePath")
    log.info(s"sourcePath  -> $sourcePath")
    val targetPath = parser.get("targetPath")
    log.info(s"targetPath  -> $targetPath")
    val mapper = new ObjectMapper()
    implicit  val resultEncoder: Encoder[Result] = Encoders.kryo(classOf[Result])

    entities.foreach{
      e =>
        val rdd =spark.sparkContext.textFile(s"$sourcePath/${e._1}").map(s => mapper.readValue(s, e._2))
        spark.createDataset(rdd).as[Result].write.mode(SaveMode.Overwrite).save(s"$targetPath/${e._1}")
    }
  }
}
