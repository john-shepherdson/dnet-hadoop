package eu.dnetlib.dhp.sx.ebi
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.{Instance, KeyValue, Oaf}
import eu.dnetlib.dhp.schema.scholexplorer.OafUtils.createQualifier
import eu.dnetlib.dhp.schema.scholexplorer.{DLIDataset, DLIRelation, OafUtils, ProvenaceInfo}
import eu.dnetlib.dhp.utils.DHPUtils
import eu.dnetlib.scholexplorer.relation.RelationMapper
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s.jackson.JsonMethods.parse

import scala.collection.JavaConverters._

object SparkAddLinkUpdates {

  val relationMapper = RelationMapper.load


case class EBILinks(relation:String, pubdate:String, tpid:String, tpidType:String, turl:String, title:String, publisher:String) {}


  def generatePubmedDLICollectedFrom(): KeyValue = {
    OafUtils.generateKeyValue("dli_________::europe_pmc__", "Europe PMC")
  }


  def ebiLinksToOaf(input:(String, String)):List[Oaf] = {
    val pmid :String = input._1
    val input_json :String = input._2
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input_json)


    val targets:List[EBILinks] = for {
      JObject(link) <- json \\ "Category" \\ "Link"
      JField("PublicationDate", JString(pubdate)) <- link
      JField("RelationshipType", JObject(relationshipType)) <- link
      JField("Name", JString(relname)) <- relationshipType
      JField("Target", JObject(target)) <- link
      JField("Identifier", JObject(identifier)) <- target
      JField("ID", JString(tpid)) <- identifier
      JField("IDScheme", JString(tpidtype)) <- identifier
      JField("IDURL", JString(turl)) <- identifier
      JField("Title", JString(title)) <- target
      JField("Publisher", JObject(pub)) <- target
      JField("Name", JString(publisher)) <- pub
    } yield EBILinks(relname, pubdate, tpid, tpidtype, turl,title, publisher)



    val dnetPublicationId = s"50|${DHPUtils.md5(s"$pmid::pmid")}"

    targets.flatMap(l => {
      val relation = new DLIRelation
      val inverseRelation = new DLIRelation
      val targetDnetId =  s"50|${DHPUtils.md5(s"${l.tpid.toLowerCase.trim}::${l.tpidType.toLowerCase.trim}")}"
      val relInfo = relationMapper.get(l.relation.toLowerCase)
      val relationSemantic = relInfo.getOriginal
      val inverseRelationSemantic = relInfo.getInverse

      relation.setSource(dnetPublicationId)
      relation.setTarget(targetDnetId)
      relation.setRelClass("datacite")
      relation.setRelType(relationSemantic)
      relation.setCollectedfrom(List(generatePubmedDLICollectedFrom()).asJava)

      inverseRelation.setSource(targetDnetId)
      inverseRelation.setTarget(dnetPublicationId)
      inverseRelation.setRelClass("datacite")
      inverseRelation.setRelType(inverseRelationSemantic)
      inverseRelation.setCollectedfrom(List(generatePubmedDLICollectedFrom()).asJava)



      val d = new DLIDataset
      d.setId(targetDnetId)
      d.setDataInfo(OafUtils.generateDataInfo())
      d.setPid(List(OafUtils.createSP(l.tpid.toLowerCase.trim, l.tpidType.toLowerCase.trim, "dnet:pid_types")).asJava)
      d.setCompletionStatus("complete")
      val pi = new ProvenaceInfo
      pi.setId("dli_________::europe_pmc__")
      pi.setName( "Europe PMC")
      pi.setCompletionStatus("complete")
      pi.setCollectionMode("collected")
      d.setDlicollectedfrom(List(pi).asJava)
      d.setCollectedfrom(List(generatePubmedDLICollectedFrom()).asJava)
      d.setPublisher(OafUtils.asField(l.publisher))
      d.setTitle(List(OafUtils.createSP(l.title, "main title", "dnet:dataCite_title")).asJava)
      d.setDateofacceptance(OafUtils.asField(l.pubdate))
      val i = new Instance
      i.setCollectedfrom(generatePubmedDLICollectedFrom())
      i.setDateofacceptance(d.getDateofacceptance)
      i.setUrl(List(l.turl).asJava)
      i.setInstancetype(createQualifier("0021", "Dataset", "dnet:publication_resource", "dnet:publication_resource"))
      d.setInstance(List(i).asJava)
      List(relation, inverseRelation, d)
    })
  }


  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(SparkCreateEBIDataFrame.getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/ebi/ebi_to_df_params.json")))
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(SparkCreateEBIDataFrame.getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()


    val workingPath = parser.get("workingPath")
    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo(classOf[Oaf])
    implicit val relEncoder: Encoder[DLIRelation] = Encoders.kryo(classOf[DLIRelation])
    implicit val datEncoder: Encoder[DLIDataset] = Encoders.kryo(classOf[DLIDataset])

    val ds:Dataset[(String,String)] = spark.read.load(s"$workingPath/baseline_links_updates").as[(String,String)](Encoders.tuple(Encoders.STRING, Encoders.STRING))

    ds.flatMap(l =>ebiLinksToOaf(l)).write.mode(SaveMode.Overwrite).save(s"$workingPath/baseline_links_updates_oaf")

    ds.filter(s => s.isInstanceOf)



    val oDataset:Dataset[Oaf] = spark.read.load(s"$workingPath/baseline_links_updates_oaf").as[Oaf]

    oDataset.filter(p =>p.isInstanceOf[DLIRelation]).map(p => p.asInstanceOf[DLIRelation]).write.mode(SaveMode.Overwrite).save(s"$workingPath/baseline_links_updates_relation")
    oDataset.filter(p =>p.isInstanceOf[DLIDataset]).map(p => p.asInstanceOf[DLIDataset]).write.mode(SaveMode.Overwrite).save(s"$workingPath/baseline_links_updates_dataset")



  }
}
