package eu.dnetlib.dhp.sx.ebi

import com.esotericsoftware.kryo.Kryo
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.schema.oaf.Result
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import eu.dnetlib.dhp.sx.ebi.model.{PMArticle, PMAuthor, PMJournal, PMParser, PubMedToOaf}
import eu.dnetlib.dhp.utils.ISLookupClientFactory
import org.apache.spark.sql.expressions.Aggregator
import org.objenesis.strategy.StdInstantiatorStrategy
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

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
    val parser = new ArgumentApplicationParser(IOUtils.toString(SparkCreateEBIDataFrame.getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/ebi/ebi_to_df_params.json")))
    parser.parseArgument(args)
    val isLookupUrl: String = parser.get("isLookupUrl")
    log.info("isLookupUrl: {}", isLookupUrl)

    val isLookupService = ISLookupClientFactory.getLookUpService(isLookupUrl)
    val vocabularies = VocabularyGroup.loadVocsFromIS(isLookupService)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(SparkCreateEBIDataFrame.getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()
    import spark.implicits._


    val sc = spark.sparkContext

    val workingPath = parser.get("workingPath")

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
      .write.mode(SaveMode.Overwrite).save(s"$workingPath/baseline_oaf")


    def extract_values(a : PMArticle):(String, String) = {
      val l:String = a.getPublicationTypes.asScala.map(p => p.getValue).mkString(",")

      (a.getPmid, l)
    }


    val ks:Dataset[(String,String)] =spark.read.load("/data/scholix/baseline_dataset").as[PMArticle].map(a => extract_values(a))(Encoders.tuple(Encoders.STRING,Encoders.STRING))

    val ids:Dataset[String] = spark.read.load("/tmp/missing_pubmed").as[String]

    ks.joinWith(ids, ks("_1").equalTo(ids("value")), "inner").map(k => k._1._2).distinct.show()




  }
}
