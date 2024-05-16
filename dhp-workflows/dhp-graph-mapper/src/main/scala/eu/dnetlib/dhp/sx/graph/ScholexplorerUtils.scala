package eu.dnetlib.dhp.sx.graph

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.schema.oaf.{KeyValue, Result, StructuredProperty}
import eu.dnetlib.dhp.schema.sx.scholix.{Scholix, ScholixCollectedFrom, ScholixEntityId, ScholixIdentifier, ScholixRelationship, ScholixResource}
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.collection.JavaConverters._
import scala.io.Source

case class RelationInfo(
  source: String,
  target: String,
  relclass: String,
  id: String,
  collectedfrom: Seq[RelKeyValue]
) {}
case class RelKeyValue(key: String, value: String) {}

object ScholexplorerUtils {

  val OPENAIRE_IDENTIFIER_SCHEMA: String = "OpenAIRE Identifier"
  val mapper= new ObjectMapper()

  case class RelationVocabulary(original: String, inverse: String) {}

  val relations: Map[String, RelationVocabulary] = {
    val input = Source
      .fromInputStream(
        getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/relation/relations.json")
      )
      .mkString
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

    lazy val json: json4s.JValue = parse(input)

    json.extract[Map[String, RelationVocabulary]]
  }

  def invRel(rel: String): String = {
    val semanticRelation = relations.getOrElse(rel.toLowerCase, null)
    if (semanticRelation != null)
      semanticRelation.inverse
    else
      null
  }

  def generateDatasourceOpenAIREURLS(id: String): String = {
    if (id != null && id.length > 12)
      s"https://explore.openaire.eu/search/dataprovider?datasourceId=${id.substring(3)}"
    else
      null
  }

  def findURLForPID(
    pidValue: List[StructuredProperty],
    urls: List[String]
  ): List[(StructuredProperty, String)] = {
    pidValue.map { p =>
      val pv = p.getValue

      val r = urls.find(u => u.toLowerCase.contains(pv.toLowerCase))
      (p, r.orNull)
    }
  }

  def extractTypedIdentifierFromInstance(r: Result): List[ScholixIdentifier] = {
    if (r.getInstance() == null || r.getInstance().isEmpty)
      return List()
    r.getInstance()
      .asScala
      .filter(i => i.getUrl != null && !i.getUrl.isEmpty)
      .filter(i => i.getPid != null && i.getUrl != null)
      .flatMap(i => findURLForPID(i.getPid.asScala.toList, i.getUrl.asScala.toList))
      .map(i => new ScholixIdentifier(i._1.getValue, i._1.getQualifier.getClassid, i._2))
      .distinct
      .toList
  }

  def generateScholixResourceFromResult(result: Result): ScholixResource = {

    if (result.getInstance() == null || result.getInstance().size() == 0)
      return null

    if (result.getPid == null || result.getPid.isEmpty)
      return null

    val r = new ScholixResource
    r.setDnetIdentifier(result.getId)

    val persistentIdentifiers: List[ScholixIdentifier] = extractTypedIdentifierFromInstance(result)
    if (persistentIdentifiers.isEmpty)
      return null

    r.setIdentifier(persistentIdentifiers.asJava)

    r.setObjectType(result.getResulttype.getClassid)

    r.setObjectSubType(
      result
        .getInstance()
        .asScala
        .filter(i => i != null && i.getInstancetype != null)
        .map(i => i.getInstancetype.getClassname)
        .distinct
        .head
    )

    if (result.getTitle != null && result.getTitle.asScala.nonEmpty) {
      val titles: List[String] = result.getTitle.asScala.map(t => t.getValue).toList
      if (titles.nonEmpty)
        r.setTitle(titles.head)
      else
        return null
    }
    if (result.getAuthor != null && !result.getAuthor.isEmpty) {
      val authors: List[ScholixEntityId] =
        result.getAuthor.asScala
          .map(a => {
            val entity = new ScholixEntityId()
            entity.setName(a.getFullname)
            if (a.getPid != null && a.getPid.size() > 0)
              entity.setIdentifiers(
                a.getPid.asScala
                  .map(sp => {
                    val id = new ScholixIdentifier()
                    id.setIdentifier(sp.getValue)
                    id.setSchema(sp.getQualifier.getClassid)
                    id
                  })
                  .take(3)
                  .toList
                  .asJava
              )
            entity
          })
          .toList
      if (authors.nonEmpty)
        r.setCreator(authors.asJava)

    }

    val dt: List[String] = result
      .getInstance()
      .asScala
      .filter(i => i.getDateofacceptance != null)
      .map(i => i.getDateofacceptance.getValue)
      .toList
    if (dt.nonEmpty)
      r.setPublicationDate(dt.distinct.head)

    r.setPublisher(
      result
        .getInstance()
        .asScala
        .map(i => i.getHostedby)
        .filter(h => !"unknown".equalsIgnoreCase(h.getValue))
        .map(h => {
          val eid = new ScholixEntityId()
          eid.setName(h.getValue)
          val id = new ScholixIdentifier()
          id.setIdentifier(h.getKey)
          id.setSchema(OPENAIRE_IDENTIFIER_SCHEMA)
          id.setUrl(generateDatasourceOpenAIREURLS(h.getKey))
          eid.setIdentifiers(List(id).asJava)
          eid
        })
        .distinct
        .asJava
    )

    r.setCollectedFrom(
      result.getCollectedfrom.asScala
        .map(cf => {
          val scf = new ScholixCollectedFrom()
          scf.setProvisionMode("collected")
          scf.setCompletionStatus("complete")
          val eid = new ScholixEntityId()
          eid.setName(cf.getValue)
          val id = new ScholixIdentifier()
          id.setIdentifier(cf.getKey)
          id.setSchema(OPENAIRE_IDENTIFIER_SCHEMA)
          id.setUrl(generateDatasourceOpenAIREURLS(cf.getKey))
          eid.setIdentifiers(List(id).asJava)
          scf.setProvider(eid)
          scf
        })
        .asJava
    )

    r
  }

  def generateScholix(relation: RelationInfo, source: ScholixResource): Scholix = {
    val s: Scholix = new Scholix
    s.setSource(source)
    if (relation.collectedfrom != null && relation.collectedfrom.nonEmpty)
      s.setLinkprovider(
        relation.collectedfrom
          .map(cf => {
            val eid = new ScholixEntityId()
            eid.setName(cf.value)
            val id = new ScholixIdentifier()
            id.setIdentifier(cf.key)
            id.setSchema(OPENAIRE_IDENTIFIER_SCHEMA)
            id.setUrl(generateDatasourceOpenAIREURLS(cf.key))
            eid.setIdentifiers(List(id).asJava)
            eid
          })
          .toList
          .asJava
      )
    else {
      val eid = new ScholixEntityId()
      eid.setName("OpenAIRE")
      val id = new ScholixIdentifier()
      id.setIdentifier("10|infrastruct_::f66f1bd369679b5b077dcdf006089556")
      id.setSchema(OPENAIRE_IDENTIFIER_SCHEMA)
      id.setUrl(generateDatasourceOpenAIREURLS(id.getIdentifier))
      eid.setIdentifiers(List(id).asJava)
      s.setLinkprovider(List(eid).asJava)
    }
    s.setIdentifier(relation.id)
    val semanticRelation = relations.getOrElse(relation.relclass.toLowerCase, null)
    if (semanticRelation == null)
      return null
    s.setRelationship(
      new ScholixRelationship(semanticRelation.original, "datacite", semanticRelation.inverse)
    )
    s.setPublicationDate(source.getPublicationDate)
    s.setPublisher(source.getPublisher)
    val mockTarget = new ScholixResource
    mockTarget.setDnetIdentifier(relation.target)
    s.setTarget(mockTarget)
    s
  }

  def updateTarget(s: Scholix, t: ScholixResource): String = {

    s.setTarget(t)
    val spublishers: Seq[ScholixEntityId] =
      if (s.getPublisher != null && !s.getPublisher.isEmpty) s.getPublisher.asScala else List()
    val tpublishers: Seq[ScholixEntityId] =
      if (t.getPublisher != null && !t.getPublisher.isEmpty) t.getPublisher.asScala else List()
    val mergedPublishers = spublishers.union(tpublishers).distinct.take(10).toList
    s.setPublisher(mergedPublishers.asJava)
    mapper.writeValueAsString(s)
  }
}
