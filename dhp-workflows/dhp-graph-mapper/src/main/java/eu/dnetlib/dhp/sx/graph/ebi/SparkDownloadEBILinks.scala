package eu.dnetlib.dhp.sx.graph.ebi

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.sx.graph.bio.BioDBToOAF.EBILinkItem
import eu.dnetlib.dhp.sx.graph.bio.pubmed.{PMArticle, PMAuthor, PMJournal}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpGet, HttpUriRequest}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object SparkDownloadEBILinks {


  def createEBILinks(pmid:Long):EBILinkItem = {

    val res = requestLinks(pmid)
    if (res!=null)
      return EBILinkItem(pmid, res)
    null
  }


  def requestPage(url:String):String = {
    val r = new HttpGet(url)
    val timeout = 60; // seconds
    val config = RequestConfig.custom()
      .setConnectTimeout(timeout * 1000)
      .setConnectionRequestTimeout(timeout * 1000)
      .setSocketTimeout(timeout * 1000).build()
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
          }
          else
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


  def requestBaseLineUpdatePage():List[String] = {
    val data =requestPage("https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/")

    val result =data.lines.filter(l => l.startsWith("<a href=")).map{l =>
      val end = l.lastIndexOf("\">")
      val start = l.indexOf("<a href=\"")

      if (start>= 0 && end >start)
        l.substring(start+9, (end-start))
      else
        ""
    }.filter(s =>s.endsWith(".gz") ).map(s => s"https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/$s").toList

    result
  }

  def downloadBaseLineUpdate(baselinePath:String, hdfsServerUri:String ):Unit = {


    val conf = new Configuration
    conf.set("fs.defaultFS", hdfsServerUri)
    val fs =  FileSystem.get(conf)
    val p = new Path((baselinePath))
    val files = fs.listFiles(p,false)

    while (files.hasNext) {
      val c = files.next()
      c.getPath

    }


  }


  def requestLinks(PMID:Long):String = {
    requestPage(s"https://www.ebi.ac.uk/europepmc/webservices/rest/MED/$PMID/datalinks?format=json")

  }
  def main(args: Array[String]): Unit = {

    val log: Logger = LoggerFactory.getLogger(getClass)
    val MAX_ITEM_PER_PARTITION = 20000
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/ebi/ebi_download_update.json")))
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(SparkEBILinksToOaf.getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()

    import spark.implicits._

    implicit  val PMEncoder: Encoder[PMArticle] = Encoders.kryo(classOf[PMArticle])
    implicit  val PMJEncoder: Encoder[PMJournal] = Encoders.kryo(classOf[PMJournal])
    implicit  val PMAEncoder: Encoder[PMAuthor] = Encoders.kryo(classOf[PMAuthor])

    val sourcePath = parser.get("sourcePath")
    log.info(s"sourcePath  -> $sourcePath")
    val workingPath = parser.get("workingPath")
    log.info(s"workingPath  -> $workingPath")

    log.info("Getting max pubmedId where the links have been requested")
    val links:Dataset[EBILinkItem] = spark.read.load(s"$sourcePath/ebi_links_dataset").as[EBILinkItem]
    val lastPMIDRequested =links.map(l => l.id).select(max("value")).first.getLong(0)

    log.info("Retrieving PMID to request links")
    val pubmed = spark.read.load(s"$sourcePath/baseline_dataset").as[PMArticle]
    pubmed.map(p => p.getPmid.toLong).where(s"value > $lastPMIDRequested").write.mode(SaveMode.Overwrite).save(s"$workingPath/id_to_request")

    val pmidToReq:Dataset[Long] = spark.read.load(s"$workingPath/id_to_request").as[Long]

    val total = pmidToReq.count()

    spark.createDataset(pmidToReq.rdd.repartition((total/MAX_ITEM_PER_PARTITION).toInt).map(pmid =>createEBILinks(pmid)).filter(l => l!= null)).write.mode(SaveMode.Overwrite).save(s"$workingPath/links_update")

    val updates:Dataset[EBILinkItem] =spark.read.load(s"$workingPath/links_update").as[EBILinkItem]

    links.union(updates).groupByKey(_.id)
      .reduceGroups{(x,y) =>
        if (x == null || x.links ==null)
          y
        if (y ==null || y.links ==null)
          x
        if (x.links.length > y.links.length)
            x
        else
          y
      }.map(_._2).write.mode(SaveMode.Overwrite).save(s"$workingPath/links_final")
  }
}
