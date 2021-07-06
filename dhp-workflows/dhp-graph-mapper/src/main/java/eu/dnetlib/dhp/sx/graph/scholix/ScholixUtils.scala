package eu.dnetlib.dhp.sx.graph.scholix


import eu.dnetlib.dhp.schema.oaf.{Dataset, Relation, Result, StructuredProperty}
import eu.dnetlib.dhp.schema.sx.scholix.{Scholix, ScholixCollectedFrom, ScholixEntityId, ScholixIdentifier, ScholixRelationship, ScholixResource}
import eu.dnetlib.dhp.schema.sx.summary.{CollectedFromType, SchemeValue, ScholixSummary, Typology}
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.collection.JavaConverters._
import scala.io.Source
import scala.language.postfixOps

object ScholixUtils {


  val DNET_IDENTIFIER_SCHEMA: String = "DNET Identifier"

  val DATE_RELATION_KEY:String = "RelationDate"
  case class RelationVocabulary(original:String, inverse:String){}

  val relations:Map[String, RelationVocabulary] = {
    val input =Source.fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/relations.json")).mkString
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

    lazy val json: json4s.JValue = parse(input)

    json.extract[Map[String, RelationVocabulary]]
  }


  def extractRelationDate(relation: Relation):String = {

    if (relation.getProperties== null || !relation.getProperties.isEmpty)
      null
    else {
      val date =relation.getProperties.asScala.find(p => DATE_RELATION_KEY.equalsIgnoreCase(p.getKey)).map(p => p.getValue)
      if (date.isDefined)
        date.get
      else
        null
    }
  }

  def extractRelationDate(summary: ScholixSummary):String = {

    if(summary.getDate== null && !summary.getDate.isEmpty)
      null
    else {
      summary.getDate.get(0)
    }


  }


  def extractCollectedFrom(summary:ScholixSummary): List[ScholixEntityId] = {
    if (summary.getDatasources!= null && !summary.getDatasources.isEmpty) {
      val l: List[ScholixEntityId] = summary.getDatasources.asScala.map{
        d => new ScholixEntityId(d.getDatasourceName, List(new ScholixIdentifier(d.getDatasourceId, "DNET Identifier", null)).asJava)
      }(collection.breakOut)
       l
    } else List()
  }

  def extractCollectedFrom(relation: Relation) : List[ScholixEntityId] = {
    if (relation.getCollectedfrom != null && !relation.getCollectedfrom.isEmpty) {


      val l: List[ScholixEntityId] = relation.getCollectedfrom.asScala.map {
        c =>

          new ScholixEntityId(c.getValue, List(new ScholixIdentifier(c.getKey, DNET_IDENTIFIER_SCHEMA,null)).asJava)
      }(collection breakOut)
      l
    } else List()
  }


  def generateScholixResourceFromSummary(summaryObject: ScholixSummary): ScholixResource = {
    val r = new ScholixResource
    r.setIdentifier(summaryObject.getLocalIdentifier)
    r.setDnetIdentifier(summaryObject.getId)

    r.setObjectType(summaryObject.getTypology.toString)
    r.setObjectSubType(summaryObject.getSubType)

    if (summaryObject.getTitle!= null && !summaryObject.getTitle.isEmpty)
        r.setTitle(summaryObject.getTitle.get(0))

    if (summaryObject.getAuthor!= null && !summaryObject.getAuthor.isEmpty){
      val l:List[ScholixEntityId] = summaryObject.getAuthor.asScala.map(a => new ScholixEntityId(a,null)).toList
      if (l.nonEmpty)
        r.setCreator(l.asJava)
    }

    if (summaryObject.getDate!= null && !summaryObject.getDate.isEmpty)
      r.setPublicationDate(summaryObject.getDate.get(0))
    if (summaryObject.getPublisher!= null && !summaryObject.getPublisher.isEmpty)
    {
      val plist:List[ScholixEntityId] =summaryObject.getPublisher.asScala.map(p => new ScholixEntityId(p, null)).toList

      if (plist.nonEmpty)
        r.setPublisher(plist.asJava)
    }


    if (summaryObject.getDatasources!= null && !summaryObject.getDatasources.isEmpty) {

      val l:List[ScholixCollectedFrom] = summaryObject.getDatasources.asScala.map(c => new ScholixCollectedFrom(
        new ScholixEntityId(c.getDatasourceName, List(new ScholixIdentifier(c.getDatasourceId, DNET_IDENTIFIER_SCHEMA, null)).asJava)
        , "collected", "complete"

      )).toList

      if (l.nonEmpty)
        r.setCollectedFrom(l.asJava)

    }
    r
  }





  def scholixFromSource(relation:Relation, source:ScholixSummary):Scholix = {

    if (relation== null || source== null)
      return null

    val s = new Scholix

    var l: List[ScholixEntityId] = extractCollectedFrom(relation)
    if (l.isEmpty)
      l = extractCollectedFrom(source)
    if (l.isEmpty)
      return null

    s.setLinkprovider(l.asJava)

    var d = extractRelationDate(relation)
    if (d == null)
      d = extractRelationDate(source)

    s.setPublicationDate(d)


    if (source.getPublisher!= null && !source.getPublisher.isEmpty) {
      val l: List[ScholixEntityId] = source.getPublisher.asScala
        .map{
          p =>
            new ScholixEntityId(p, null)
        }(collection.breakOut)

      if (l.nonEmpty)
        s.setPublisher(l.asJava)
    }

    val semanticRelation = relations.getOrElse(relation.getRelClass.toLowerCase, null)
    if (semanticRelation== null)
      return null
    s.setRelationship(new ScholixRelationship(semanticRelation.original, "datacite", semanticRelation.inverse))
    s.setSource(generateScholixResourceFromSummary(source))

    s
  }


  def findURLForPID(pidValue:List[StructuredProperty], urls:List[String]):List[(StructuredProperty, String)] = {
    pidValue.map{
      p =>
        val pv = p.getValue

        val r = urls.find(u => u.toLowerCase.contains(pv.toLowerCase))
        (p, r.orNull)
    }
  }


  def extractTypedIdentifierFromInstance(r:Result):List[ScholixIdentifier] = {
    if (r.getInstance() == null || r.getInstance().isEmpty)
      return List()
    r.getInstance().asScala.filter(i => i.getUrl!= null && !i.getUrl.isEmpty)

      .flatMap(i => findURLForPID(i.getPid.asScala.toList, i.getUrl.asScala.toList))
      .map(i => new ScholixIdentifier(i._1.getValue, i._1.getQualifier.getClassid, i._2)).distinct.toList
  }

  def resultToSummary(r:Result):ScholixSummary = {
    val s = new ScholixSummary
    s.setId(r.getId)
    if (r.getPid == null || r.getPid.isEmpty)
      return null

    val pids:List[ScholixIdentifier] =  extractTypedIdentifierFromInstance(r)
    if (pids.isEmpty)
      return null
    s.setLocalIdentifier(pids.asJava)
    if (r.isInstanceOf[Dataset])
      s.setTypology(Typology.dataset)
    else
      s.setTypology(Typology.publication)

    s.setSubType(r.getInstance().get(0).getInstancetype.getClassname)

    if (r.getTitle!= null && r.getTitle.asScala.nonEmpty) {
      val titles:List[String] =r.getTitle.asScala.map(t => t.getValue)(collection breakOut)
      if (titles.nonEmpty)
        s.setTitle(titles.asJava)
      else
        return  null
    }

    if(r.getAuthor!= null && !r.getAuthor.isEmpty) {
      val authors:List[String] = r.getAuthor.asScala.map(a=> a.getFullname)(collection breakOut)
      if (authors nonEmpty)
        s.setAuthor(authors.asJava)
    }
    if (r.getInstance() != null) {
      val dt:List[String] = r.getInstance().asScala.filter(i => i.getDateofacceptance != null).map(i => i.getDateofacceptance.getValue)(collection.breakOut)
      if (dt.nonEmpty)
        s.setDate(dt.distinct.asJava)
    }
    if (r.getDescription!= null && !r.getDescription.isEmpty) {
      val d = r.getDescription.asScala.find(f => f.getValue!=null)
      if (d.isDefined)
        s.setDescription(d.get.getValue)
    }

    if (r.getSubject!= null && !r.getSubject.isEmpty) {
      val subjects:List[SchemeValue] =r.getSubject.asScala.map(s => new SchemeValue(s.getQualifier.getClassname, s.getValue))(collection breakOut)
      if (subjects.nonEmpty)
        s.setSubject(subjects.asJava)
    }

    if (r.getPublisher!= null)
      s.setPublisher(List(r.getPublisher.getValue).asJava)

    if (r.getCollectedfrom!= null && !r.getCollectedfrom.isEmpty) {
      val cf:List[CollectedFromType] = r.getCollectedfrom.asScala.map(c => new CollectedFromType(c.getValue, c.getKey, "complete"))(collection breakOut)
      if (cf.nonEmpty)
        s.setDatasources(cf.distinct.asJava)
    }

    s.setRelatedDatasets(0)
    s.setRelatedPublications(0)
    s.setRelatedUnknown(0)

    s
  }

}
