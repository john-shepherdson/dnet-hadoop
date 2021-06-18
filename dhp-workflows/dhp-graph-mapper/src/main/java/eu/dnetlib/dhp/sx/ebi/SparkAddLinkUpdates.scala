package eu.dnetlib.dhp.sx.ebi
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.{Author, Instance, Journal, KeyValue, Oaf, Publication, Relation, Dataset => OafDataset}
import eu.dnetlib.dhp.schema.scholexplorer.OafUtils.createQualifier
import eu.dnetlib.dhp.schema.scholexplorer.{DLIDataset, DLIPublication, OafUtils, ProvenaceInfo}
import eu.dnetlib.dhp.sx.bio.pubmed.{PMArticle, PMAuthor, PMJournal}
import eu.dnetlib.dhp.utils.DHPUtils
import eu.dnetlib.scholexplorer.relation.RelationMapper
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s.jackson.JsonMethods.parse
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

object SparkAddLinkUpdates {

  val relationMapper: RelationMapper = RelationMapper.load


case class EBILinks(relation:String, pubdate:String, tpid:String, tpidType:String, turl:String, title:String, publisher:String) {}


  def generatePubmedDLICollectedFrom(): KeyValue = {
    OafUtils.generateKeyValue("dli_________::europe_pmc__", "Europe PMC")
  }



  def journalToOAF(pj:PMJournal): Journal = {
    val j = new Journal
    j.setIssnPrinted(pj.getIssn)
    j.setVol(pj.getVolume)
    j.setName(pj.getTitle)
    j.setIss(pj.getIssue)
    j.setDataInfo(OafUtils.generateDataInfo())
    j
  }


  def pubmedTOPublication(input:PMArticle):DLIPublication = {


    val dnetPublicationId = s"50|${DHPUtils.md5(s"${input.getPmid}::pmid")}"

    val p = new DLIPublication
    p.setId(dnetPublicationId)
    p.setDataInfo(OafUtils.generateDataInfo())
    p.setPid(List(OafUtils.createSP(input.getPmid.toLowerCase.trim, "pmid", ModelConstants.DNET_PID_TYPES)).asJava)
    p.setCompletionStatus("complete")
    val pi = new ProvenaceInfo
    pi.setId("dli_________::europe_pmc__")
    pi.setName( "Europe PMC")
    pi.setCompletionStatus("complete")
    pi.setCollectionMode("collected")
    p.setDlicollectedfrom(List(pi).asJava)
    p.setCollectedfrom(List(generatePubmedDLICollectedFrom()).asJava)

    if (input.getAuthors != null && input.getAuthors.size() >0) {
      var aths: List[Author] = List()
      input.getAuthors.asScala.filter(a=> a!= null).foreach(a => {
        val c = new Author
        c.setFullname(a.getFullName)
        c.setName(a.getForeName)
        c.setSurname(a.getLastName)
        aths =  aths ::: List(c)
      })
      if (aths.nonEmpty)
        p.setAuthor(aths.asJava)
    }


    if (input.getJournal != null)
      p.setJournal(journalToOAF(input.getJournal))
    p.setTitle(List(OafUtils.createSP(input.getTitle, "main title", ModelConstants.DNET_DATACITE_TITLE)).asJava)
    p.setDateofacceptance(OafUtils.asField(input.getDate))
    val i = new Instance
    i.setCollectedfrom(generatePubmedDLICollectedFrom())
    i.setDateofacceptance(p.getDateofacceptance)
    i.setUrl(List(s"https://pubmed.ncbi.nlm.nih.gov/${input.getPmid}").asJava)
    i.setInstancetype(createQualifier("0001", "Article", ModelConstants.DNET_PUBLICATION_RESOURCE, ModelConstants.DNET_PUBLICATION_RESOURCE))
    p.setInstance(List(i).asJava)
    p
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
      val relation = new Relation
      val inverseRelation = new Relation
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
      d.setPid(List(OafUtils.createSP(l.tpid.toLowerCase.trim, l.tpidType.toLowerCase.trim, ModelConstants.DNET_PID_TYPES)).asJava)
      d.setCompletionStatus("complete")
      val pi = new ProvenaceInfo
      pi.setId("dli_________::europe_pmc__")
      pi.setName( "Europe PMC")
      pi.setCompletionStatus("complete")
      pi.setCollectionMode("collected")
      d.setDlicollectedfrom(List(pi).asJava)
      d.setCollectedfrom(List(generatePubmedDLICollectedFrom()).asJava)
      d.setPublisher(OafUtils.asField(l.publisher))
      d.setTitle(List(OafUtils.createSP(l.title, "main title", ModelConstants.DNET_DATACITE_TITLE)).asJava)
      d.setDateofacceptance(OafUtils.asField(l.pubdate))
      val i = new Instance
      i.setCollectedfrom(generatePubmedDLICollectedFrom())
      i.setDateofacceptance(d.getDateofacceptance)
      i.setUrl(List(l.turl).asJava)
      i.setInstancetype(createQualifier("0021", "Dataset", ModelConstants.DNET_PUBLICATION_RESOURCE, ModelConstants.DNET_PUBLICATION_RESOURCE))
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
    implicit val oafpubEncoder: Encoder[Publication] = Encoders.kryo[Publication]
    implicit val relEncoder: Encoder[Relation] = Encoders.kryo(classOf[Relation])
    implicit val datEncoder: Encoder[DLIDataset] = Encoders.kryo(classOf[DLIDataset])
    implicit val pubEncoder: Encoder[DLIPublication] = Encoders.kryo(classOf[DLIPublication])
    implicit val atEncoder: Encoder[Author] = Encoders.kryo(classOf[Author])
    implicit  val strEncoder:Encoder[String] = Encoders.STRING
    implicit  val PMEncoder: Encoder[PMArticle] = Encoders.kryo(classOf[PMArticle])
    implicit  val PMJEncoder: Encoder[PMJournal] = Encoders.kryo(classOf[PMJournal])
    implicit  val PMAEncoder: Encoder[PMAuthor] = Encoders.kryo(classOf[PMAuthor])


    val ds:Dataset[(String,String)] = spark.read.load(s"$workingPath/baseline_links_updates").as[(String,String)](Encoders.tuple(Encoders.STRING, Encoders.STRING))

    ds.flatMap(l =>ebiLinksToOaf(l)).write.mode(SaveMode.Overwrite).save(s"$workingPath/baseline_links_updates_oaf")

    ds.filter(s => s.isInstanceOf)



    val oDataset:Dataset[Oaf] = spark.read.load(s"$workingPath/baseline_links_updates_oaf").as[Oaf]

    oDataset.filter(p =>p.isInstanceOf[Relation]).map(p => p.asInstanceOf[Relation]).write.mode(SaveMode.Overwrite).save(s"$workingPath/baseline_links_updates_relation")
    oDataset.filter(p =>p.isInstanceOf[DLIDataset]).map(p => p.asInstanceOf[DLIDataset]).write.mode(SaveMode.Overwrite).save(s"$workingPath/baseline_links_updates_dataset")


    val idPublicationSolved:Dataset[String] = spark.read.load(s"$workingPath/baseline_links_updates").where(col("links").isNotNull).select("pmid").as[String]
    val baseline:Dataset[(String, PMArticle)]= spark.read.load(s"$workingPath/baseline_dataset").as[PMArticle].map(p=> (p.getPmid, p))(Encoders.tuple(strEncoder,PMEncoder))
    idPublicationSolved.joinWith(baseline, idPublicationSolved("pmid").equalTo(baseline("_1"))).map(k => pubmedTOPublication(k._2._2)).write.mode(SaveMode.Overwrite).save(s"$workingPath/baseline_links_updates_publication")


    val pmaDatasets = spark.read.load("/user/sandro.labruzzo/scholix/EBI/ebi_garr/baseline_dataset").as[PMArticle]

    pmaDatasets.map(p => pubmedTOPublication(p)).write.mode(SaveMode.Overwrite).save(s"$workingPath/baseline_publication_all")

    val pubs: Dataset[(String,Publication)] = spark.read.load("/user/sandro.labruzzo/scholix/EBI/publication").as[Publication].map(p => (p.getId, p))(Encoders.tuple(Encoders.STRING,oafpubEncoder))
    val pubdate:Dataset[(String,DLIPublication)] = spark.read.load(s"$workingPath/baseline_publication_all").as[DLIPublication].map(p => (p.getId, p))(Encoders.tuple(Encoders.STRING,pubEncoder))



    pubs.joinWith(pubdate, pubs("_1").equalTo(pubdate("_1"))).map(k => k._2._2).write.mode(SaveMode.Overwrite).save(s"$workingPath/baseline_publication_ebi")



    val dt : Dataset[DLIDataset] = spark.read.load(s"$workingPath/dataset").as[DLIDataset]
    val update : Dataset[DLIDataset] = spark.read.load(s"$workingPath/ebi_garr/baseline_links_updates_dataset").as[DLIDataset]


    dt.union(update).map(d => (d.getId,d))(Encoders.tuple(Encoders.STRING, datEncoder))
      .groupByKey(_._1)(Encoders.STRING)
      .agg(EBIAggregator.getDLIDatasetAggregator().toColumn)
      .map(p => p._2)
      .write.mode(SaveMode.Overwrite).save(s"$workingPath/baseline_dataset_ebi")


    val rel: Dataset[Relation] = spark.read.load(s"$workingPath/relation").as[Relation]
    val relupdate : Dataset[Relation] = spark.read.load(s"$workingPath/ebi_garr/baseline_links_updates_relation").as[Relation]


    rel.union(relupdate)
      .map(d => (s"${d.getSource}::${d.getRelType}::${d.getTarget}", d))(Encoders.tuple(Encoders.STRING, relEncoder))
      .groupByKey(_._1)(Encoders.STRING)
      .agg(EBIAggregator.getRelationAggregator().toColumn)
      .map(p => p._2)
      .write.mode(SaveMode.Overwrite)
      .save(s"$workingPath/baseline_relation_ebi")

  }
}
