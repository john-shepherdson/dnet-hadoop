package eu.dnetlib.dhp.sx.bio.ebi

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.collection.CollectionUtils
import eu.dnetlib.dhp.common.Constants.{MDSTORE_DATA_PATH, MDSTORE_SIZE_PATH}
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.mdstore.MDStoreVersion
import eu.dnetlib.dhp.schema.oaf.{Oaf, Result}
import eu.dnetlib.dhp.sx.bio.pubmed._
import eu.dnetlib.dhp.utils.DHPUtils.{MAPPER, writeHdfsFile}
import eu.dnetlib.dhp.utils.ISLookupClientFactory
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

import java.io.InputStream
import scala.io.Source
import scala.xml.pull.XMLEventReader

object SparkCreateBaselineDataFrame {

  def requestBaseLineUpdatePage(maxFile: String): List[(String, String)] = {
    val data = requestPage("https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/")

    val result = data.linesWithSeparators
      .map(l => l.stripLineEnd)
      .filter(l => l.startsWith("<a href="))
      .map { l =>
        val end = l.lastIndexOf("\">")
        val start = l.indexOf("<a href=\"")

        if (start >= 0 && end > start)
          l.substring(start + 9, end - start)
        else
          ""
      }
      .filter(s => s.endsWith(".gz"))
      .filter(s => s > maxFile)
      .map(s => (s, s"https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/$s"))
      .toList

    result
  }

  def downloadBaselinePart(url: String): InputStream = {
    val r = new HttpGet(url)
    val timeout = 60; // seconds
    val config = RequestConfig
      .custom()
      .setConnectTimeout(timeout * 1000)
      .setConnectionRequestTimeout(timeout * 1000)
      .setSocketTimeout(timeout * 1000)
      .build()
    val client = HttpClientBuilder.create().setDefaultRequestConfig(config).build()
    println(s"Downloading ${url}")
    val response = client.execute(r)
    println(s"got response with status: ${response.getStatusLine.getStatusCode}")
    response.getEntity.getContent

  }

  def requestPage(url: String): String = {
    val r = new HttpGet(url)
    val timeout = 60; // seconds
    val config = RequestConfig
      .custom()
      .setConnectTimeout(timeout * 1000)
      .setConnectionRequestTimeout(timeout * 1000)
      .setSocketTimeout(timeout * 1000)
      .build()
    val client = HttpClientBuilder.create().setDefaultRequestConfig(config).build()
    try {
      var tries = 4
      while (tries > 0) {
        println(s"requesting ${r.getURI}")
        try {
          val response = client.execute(r)
          println(s"get response with status${response.getStatusLine.getStatusCode}")
          if (response.getStatusLine.getStatusCode > 400) {
            tries -= 1
          } else
            return IOUtils.toString(response.getEntity.getContent)
        } catch {
          case e: Throwable =>
            println(s"Error on requesting ${r.getURI}")
            e.printStackTrace()
            tries -= 1
        }
      }
      ""
    } finally {
      if (client != null)
        client.close()
    }
  }

  def downloadBaseLineUpdate(baselinePath: String, hdfsServerUri: String): Unit = {

    val conf = new Configuration
    conf.set("fs.defaultFS", hdfsServerUri)
    val fs = FileSystem.get(conf)
    val p = new Path(baselinePath)
    val files = fs.listFiles(p, false)
    var max_file = ""
    while (files.hasNext) {
      val c = files.next()
      val data = c.getPath.toString
      val fileName = data.substring(data.lastIndexOf("/") + 1)

      if (fileName > max_file)
        max_file = fileName
    }

    val files_to_download = requestBaseLineUpdatePage(max_file)

    files_to_download.foreach { u =>
      val hdfsWritePath: Path = new Path(s"$baselinePath/${u._1}")
      val fsDataOutputStream: FSDataOutputStream = fs.create(hdfsWritePath, true)
      val i = downloadBaselinePart(u._2)
      IOUtils.copy(i, fsDataOutputStream)
      println(s"Saved file ${u._2} in path $baselinePath/${u._1}")
      fsDataOutputStream.close()
    }

  }

  val pmArticleAggregator: Aggregator[(String, PMArticle), PMArticle, PMArticle] =
    new Aggregator[(String, PMArticle), PMArticle, PMArticle] with Serializable {
      override def zero: PMArticle = new PMArticle

      override def reduce(b: PMArticle, a: (String, PMArticle)): PMArticle = {
        if (b != null && b.getPmid != null) b else a._2
      }

      override def merge(b1: PMArticle, b2: PMArticle): PMArticle = {
        if (b1 != null && b1.getPmid != null) b1 else b2

      }

      override def finish(reduction: PMArticle): PMArticle = reduction

      override def bufferEncoder: Encoder[PMArticle] = Encoders.kryo[PMArticle]

      override def outputEncoder: Encoder[PMArticle] = Encoders.kryo[PMArticle]
    }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    val log: Logger = LoggerFactory.getLogger(getClass)
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        SparkEBILinksToOaf.getClass.getResourceAsStream(
          "/eu/dnetlib/dhp/sx/bio/ebi/baseline_to_oaf_params.json"
        )
      )
    )
    parser.parseArgument(args)
    val isLookupUrl: String = parser.get("isLookupUrl")
    log.info("isLookupUrl: {}", isLookupUrl)
    val workingPath = parser.get("workingPath")
    log.info("workingPath: {}", workingPath)

    val mdstoreOutputVersion = parser.get("mdstoreOutputVersion")
    log.info("mdstoreOutputVersion: {}", mdstoreOutputVersion)

    val cleanedMdStoreVersion = MAPPER.readValue(mdstoreOutputVersion, classOf[MDStoreVersion])
    val outputBasePath = cleanedMdStoreVersion.getHdfsPath
    log.info("outputBasePath: {}", outputBasePath)

    val hdfsServerUri = parser.get("hdfsServerUri")
    log.info("hdfsServerUri: {}", hdfsServerUri)

    val skipUpdate = parser.get("skipUpdate")
    log.info("skipUpdate: {}", skipUpdate)

    val isLookupService = ISLookupClientFactory.getLookUpService(isLookupUrl)
    val vocabularies = VocabularyGroup.loadVocsFromIS(isLookupService)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(SparkEBILinksToOaf.getClass.getSimpleName)
        .master(parser.get("master"))
        .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    implicit val PMEncoder: Encoder[PMArticle] = Encoders.kryo(classOf[PMArticle])
    implicit val PMJEncoder: Encoder[PMJournal] = Encoders.kryo(classOf[PMJournal])
    implicit val PMAEncoder: Encoder[PMAuthor] = Encoders.kryo(classOf[PMAuthor])
    implicit val resultEncoder: Encoder[Oaf] = Encoders.kryo(classOf[Oaf])

    if (!"true".equalsIgnoreCase(skipUpdate)) {
      downloadBaseLineUpdate(s"$workingPath/baseline", hdfsServerUri)
      val k: RDD[(String, String)] = sc.wholeTextFiles(s"$workingPath/baseline", 2000)
      val ds: Dataset[PMArticle] = spark.createDataset(
        k.filter(i => i._1.endsWith(".gz"))
          .flatMap(i => {
            val xml = new XMLEventReader(Source.fromBytes(i._2.getBytes()))
            new PMParser(xml)
          })
      )
      ds.map(p => (p.getPmid, p))(Encoders.tuple(Encoders.STRING, PMEncoder))
        .groupByKey(_._1)
        .agg(pmArticleAggregator.toColumn)
        .map(p => p._2)
        .write
        .mode(SaveMode.Overwrite)
        .save(s"$workingPath/baseline_dataset")
    }

    log.info(s"saved dataset: '$workingPath/baseline_dataset'")

    val exported_dataset = spark.read.load(s"$workingPath/baseline_dataset").as[PMArticle]
    CollectionUtils.saveDataset(
      exported_dataset
        .map(a => PubMedToOaf.convert(a, vocabularies))
        .as[Oaf]
        .filter(p => p != null),
      s"$outputBasePath/$MDSTORE_DATA_PATH"
    )

    val df = spark.read.text(s"$outputBasePath/$MDSTORE_DATA_PATH")
    val mdStoreSize = df.count
    writeHdfsFile(spark.sparkContext.hadoopConfiguration, s"$mdStoreSize", s"$outputBasePath/$MDSTORE_SIZE_PATH")
  }
}
