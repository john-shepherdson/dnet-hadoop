package eu.dnetlib.dhp.datacite

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.io.{IntWritable, SequenceFile, Text}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{Dataset, Encoder, SaveMode, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.{Logger, LoggerFactory}

import java.time.format.DateTimeFormatter.ISO_DATE_TIME
import java.time.{LocalDateTime, ZoneOffset}
import scala.io.Source

object ImportDatacite {

  val log: Logger = LoggerFactory.getLogger(ImportDatacite.getClass)

  def convertAPIStringToDataciteItem(input: String): DataciteType = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: org.json4s.JValue = parse(input)
    val doi = (json \ "attributes" \ "doi").extract[String].toLowerCase

    val isActive = (json \ "attributes" \ "isActive").extract[Boolean]

    val timestamp_string = (json \ "attributes" \ "updated").extract[String]
    val dt = LocalDateTime.parse(timestamp_string, ISO_DATE_TIME)
    DataciteType(
      doi = doi,
      timestamp = dt.toInstant(ZoneOffset.UTC).toEpochMilli / 1000,
      isActive = isActive,
      json = input
    )

  }

  def main(args: Array[String]): Unit = {

    val parser = new ArgumentApplicationParser(
      Source
        .fromInputStream(
          getClass.getResourceAsStream(
            "/eu/dnetlib/dhp/datacite/import_from_api.json"
          )
        )
        .mkString
    )
    parser.parseArgument(args)
    val master = parser.get("master")

    val hdfsuri = parser.get("namenode")
    log.info(s"namenode is $hdfsuri")

    val targetPath = parser.get("targetPath")
    log.info(s"targetPath is $targetPath")

    val dataciteDump = parser.get("dataciteDumpPath")
    log.info(s"dataciteDump is $dataciteDump")

    val hdfsTargetPath = new Path(targetPath)
    log.info(s"hdfsTargetPath is $hdfsTargetPath")

    val bs = if (parser.get("blocksize") == null) 100 else parser.get("blocksize").toInt

    val spkipImport = parser.get("skipImport")
    log.info(s"skipImport is $spkipImport")

    val spark: SparkSession = SparkSession
      .builder()
      .appName(ImportDatacite.getClass.getSimpleName)
      .master(master)
      .getOrCreate()

    // ====== Init HDFS File System Object
    val conf = new Configuration
    // Set FileSystem URI
    conf.set("fs.defaultFS", hdfsuri)

    // Because of Maven
    conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    conf.set("fs.file.impl", classOf[LocalFileSystem].getName)
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._

    val dataciteAggregator: Aggregator[DataciteType, DataciteType, DataciteType] =
      new Aggregator[DataciteType, DataciteType, DataciteType] with Serializable {

        override def zero: DataciteType = null

        override def reduce(a: DataciteType, b: DataciteType): DataciteType = {
          if (b == null)
            return a
          if (a == null)
            return b
          if (a.timestamp > b.timestamp) {
            return a
          }
          b
        }

        override def merge(a: DataciteType, b: DataciteType): DataciteType = {
          reduce(a, b)
        }

        override def bufferEncoder: Encoder[DataciteType] = implicitly[Encoder[DataciteType]]

        override def outputEncoder: Encoder[DataciteType] = implicitly[Encoder[DataciteType]]

        override def finish(reduction: DataciteType): DataciteType = reduction
      }

    val dump: Dataset[DataciteType] = spark.read.load(dataciteDump).as[DataciteType]
    val ts = dump.select(max("timestamp")).first().getLong(0)

    println(s"last Timestamp is $ts")

    val cnt =
      if ("true".equalsIgnoreCase(spkipImport)) 1
      else writeSequenceFile(hdfsTargetPath, ts, conf, bs)

    println(s"Imported from Datacite API $cnt documents")

    if (cnt > 0) {

      val inputRdd: RDD[DataciteType] = sc
        .sequenceFile(targetPath, classOf[Int], classOf[Text])
        .map(s => s._2.toString)
        .map(s => convertAPIStringToDataciteItem(s))
      spark.createDataset(inputRdd).write.mode(SaveMode.Overwrite).save(s"${targetPath}_dataset")

      val ds: Dataset[DataciteType] = spark.read.load(s"${targetPath}_dataset").as[DataciteType]

      dump
        .union(ds)
        .groupByKey(_.doi)
        .agg(dataciteAggregator.toColumn)
        .map(s => s._2)
        .repartition(4000)
        .write
        .mode(SaveMode.Overwrite)
        .save(s"${dataciteDump}_updated")

      val fs = FileSystem.get(sc.hadoopConfiguration)
      fs.delete(new Path(s"$dataciteDump"), true)
      fs.rename(new Path(s"${dataciteDump}_updated"), new Path(s"$dataciteDump"))
    }
  }

  private def writeSequenceFile(
    hdfsTargetPath: Path,
    timestamp: Long,
    conf: Configuration,
    bs: Int
  ): Long = {
    var from: Long = timestamp * 1000
    val delta: Long = 100000000L
    var client: DataciteAPIImporter = null
    val now: Long = System.currentTimeMillis()
    var i = 0
    try {
      val writer = SequenceFile.createWriter(
        conf,
        SequenceFile.Writer.file(hdfsTargetPath),
        SequenceFile.Writer.keyClass(classOf[IntWritable]),
        SequenceFile.Writer.valueClass(classOf[Text])
      )
      try {
        var start: Long = System.currentTimeMillis
        while (from < now) {
          client = new DataciteAPIImporter(from, bs, from + delta)
          var end: Long = 0
          val key: IntWritable = new IntWritable(i)
          val value: Text = new Text
          while (client.hasNext) {
            key.set {
              i += 1;
              i - 1
            }
            value.set(client.next())
            writer.append(key, value)
            writer.hflush()
            if (i % 1000 == 0) {
              end = System.currentTimeMillis
              val time = (end - start) / 1000.0f
              println(s"Imported $i in $time seconds")
              start = System.currentTimeMillis
            }
          }
          println(s"updating from value: $from  -> ${from + delta}")
          from = from + delta
        }
      } catch {
        case e: Throwable =>
          println("Error", e)
      } finally if (writer != null) writer.close()
    } catch {
      case e: Throwable =>
        log.error("Error", e)
    }
    i
  }

}
