package eu.dnetlib.dhp.doiboost
import eu.dnetlib.dhp.schema.oaf.{Publication, Relation, StructuredProperty, Dataset => OafDataset}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import scala.::
import scala.collection.JavaConverters._
class QueryTest {


  def extractLicense(p:Publication):Tuple2[String,String] = {

      val tmp = p.getInstance().asScala.map(i => i.getLicense.getValue).distinct.mkString(",")
      (p.getId,tmp)
    }



  def hasDOI(publication: Publication, doi:String):Boolean = {


    val s = publication.getOriginalId.asScala.filter(i =>  i.equalsIgnoreCase(doi))

    s.nonEmpty

  }

  def hasNullHostedBy(publication: Publication):Boolean = {
    publication.getInstance().asScala.exists(i => i.getHostedby == null || i.getHostedby.getValue == null)
  }



  def myQuery(spark:SparkSession): Unit = {
    implicit val mapEncoderPub: Encoder[Publication] = Encoders.kryo[Publication]
    implicit val mapEncoderDat: Encoder[OafDataset] = Encoders.kryo[OafDataset]
    implicit val mapEncoderRel: Encoder[Relation] = Encoders.kryo[Relation]

    val doiboostPubs:Dataset[Publication]    = spark.read.load("/data/doiboost/process/doiBoostPublicationFiltered").as[Publication]

    val relFunder: Dataset[Relation] = spark.read.format("org.apache.spark.sql.parquet").load("/data/doiboost/process/crossrefRelation").as[Relation]

    doiboostPubs.filter(p => p.getDateofacceptance != null && p.getDateofacceptance.getValue!= null && p.getDateofacceptance.getValue.length > 0 )

    doiboostPubs.filter(p=>hasDOI(p, "10.1016/j.is.2020.101522")).collect()(0).getDescription.get(0).getValue



    doiboostPubs.filter(p=> hasNullHostedBy(p)).count()

    doiboostPubs.map(p=> (p.getId, p.getBestaccessright.getClassname))(Encoders.tuple(Encoders.STRING,Encoders.STRING))
  }

}
