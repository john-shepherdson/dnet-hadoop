package eu.dnetlib.dhp.sx.graph.ebi

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.oaf.Result
import eu.dnetlib.dhp.sx.graph.bio.pubmed.{PMArticle, PMAuthor, PMJournal, PMParser, PubMedToOaf}
import eu.dnetlib.dhp.utils.ISLookupClientFactory
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source
import scala.xml.pull.XMLEventReader

object SparkCreateBaselineDataFrame {


  val pmArticleAggregator: Aggregator[(String, PMArticle), PMArticle, PMArticle] = new Aggregator[(String, PMArticle), PMArticle, PMArticle] with Serializable {
    override def zero: PMArticle = new PMArticle

    override def reduce(b: PMArticle, a: (String, PMArticle)): PMArticle = {
      if (b != null && b.getPmid!= null)   b  else a._2
    }

    override def merge(b1: PMArticle, b2: PMArticle): PMArticle = {
      if (b1 != null && b1.getPmid!= null)    b1   else     b2

    }

    override def finish(reduction: PMArticle): PMArticle = reduction

    override def bufferEncoder: Encoder[PMArticle] = Encoders.kryo[PMArticle]

    override def outputEncoder: Encoder[PMArticle] = Encoders.kryo[PMArticle]
  }


  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    val log: Logger = LoggerFactory.getLogger(getClass)
    val parser = new ArgumentApplicationParser(IOUtils.toString(SparkEBILinksToOaf.getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/ebi/baseline_to_oaf_params.json")))
    parser.parseArgument(args)
    val isLookupUrl: String = parser.get("isLookupUrl")
    log.info("isLookupUrl: {}", isLookupUrl)
    val workingPath = parser.get("workingPath")
    log.info("workingPath: {}", workingPath)

    val targetPath = parser.get("targetPath")
    log.info("targetPath: {}", targetPath)

    val isLookupService = ISLookupClientFactory.getLookUpService(isLookupUrl)
    val vocabularies = VocabularyGroup.loadVocsFromIS(isLookupService)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(SparkEBILinksToOaf.getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()
    import spark.implicits._


    val sc = spark.sparkContext



    implicit  val PMEncoder: Encoder[PMArticle] = Encoders.kryo(classOf[PMArticle])
    implicit  val PMJEncoder: Encoder[PMJournal] = Encoders.kryo(classOf[PMJournal])
    implicit  val PMAEncoder: Encoder[PMAuthor] = Encoders.kryo(classOf[PMAuthor])
    implicit  val resultEncoder: Encoder[Result] = Encoders.kryo(classOf[Result])

    val k: RDD[(String, String)] = sc.wholeTextFiles(s"$workingPath/baseline",2000)
    val ds:Dataset[PMArticle] = spark.createDataset(k.filter(i => i._1.endsWith(".gz")).flatMap(i =>{
      val xml = new XMLEventReader(Source.fromBytes(i._2.getBytes()))
      new PMParser(xml)

    } ))

    ds.map(p => (p.getPmid,p))(Encoders.tuple(Encoders.STRING, PMEncoder)).groupByKey(_._1)
      .agg(pmArticleAggregator.toColumn)
      .map(p => p._2).write.mode(SaveMode.Overwrite).save(s"$workingPath/baseline_dataset")

    val exported_dataset = spark.read.load(s"$workingPath/baseline_dataset").as[PMArticle]
    exported_dataset
      .map(a => PubMedToOaf.convert(a, vocabularies)).as[Result]
      .filter(p => p!= null)
      .write.mode(SaveMode.Overwrite).save(targetPath)

    //s"$workingPath/oaf/baseline_oaf"
  }
}
