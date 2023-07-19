package eu.dnetlib.dhp.sx.graph.scholix

import eu.dnetlib.dhp.schema.oaf.{Publication, Relation, Result, StructuredProperty}
import eu.dnetlib.dhp.schema.sx.scholix._
import eu.dnetlib.dhp.schema.sx.summary.{CollectedFromType, SchemeValue, ScholixSummary, Typology}
import eu.dnetlib.dhp.utils.DHPUtils
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import scala.collection.JavaConverters._
import scala.io.Source

object ScholixUtils extends Serializable {

  val DNET_IDENTIFIER_SCHEMA: String = "DNET Identifier"

  val DATE_RELATION_KEY: String = "RelationDate"

  case class RelationVocabulary(original: String, inverse: String) {}

  case class RelatedEntities(id: String, relatedDataset: Long, relatedPublication: Long) {}

  val relations: Map[String, RelationVocabulary] = {
    val input = Source
      .fromInputStream(
        getClass.getResourceAsStream("/eu/dnetlib/scholexplorer/relation/relations.json")
      )
      .mkString
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

    lazy val json: json4s.JValue = parse(input)

    json.extract[Map[String, RelationVocabulary]]
  }

  def extractRelationDate(relation: Relation): String = {

    if (relation.getProperties == null || !relation.getProperties.isEmpty)
      null
    else {
      val date = relation.getProperties.asScala
        .find(p => DATE_RELATION_KEY.equalsIgnoreCase(p.getKey))
        .map(p => p.getValue)
      if (date.isDefined)
        date.get
      else
        null
    }
  }

  def extractRelationDate(summary: ScholixResource): String = {

    summary.getPublicationDate

  }

  def inverseRelationShip(rel: ScholixRelationship): ScholixRelationship = {
    new ScholixRelationship(rel.getInverse, rel.getSchema, rel.getName)

  }

  def generateScholixResourceFromResult(r: Result): ScholixResource = {
    generateScholixResourceFromSummary(ScholixUtils.resultToSummary(r))
  }

  val statsAggregator: Aggregator[(String, String, Long), RelatedEntities, RelatedEntities] =
    new Aggregator[(String, String, Long), RelatedEntities, RelatedEntities] with Serializable {
      override def zero: RelatedEntities = null

      override def reduce(b: RelatedEntities, a: (String, String, Long)): RelatedEntities = {
        val relatedDataset = if ("dataset".equalsIgnoreCase(a._2)) a._3 else 0
        val relatedPublication = if ("publication".equalsIgnoreCase(a._2)) a._3 else 0

        if (b == null)
          RelatedEntities(a._1, relatedDataset, relatedPublication)
        else
          RelatedEntities(
            a._1,
            b.relatedDataset + relatedDataset,
            b.relatedPublication + relatedPublication
          )
      }

      override def merge(b1: RelatedEntities, b2: RelatedEntities): RelatedEntities = {
        if (b1 != null && b2 != null)
          RelatedEntities(
            b1.id,
            b1.relatedDataset + b2.relatedDataset,
            b1.relatedPublication + b2.relatedPublication
          )
        else if (b1 != null)
          b1
        else
          b2
      }

      override def finish(reduction: RelatedEntities): RelatedEntities = reduction

      override def bufferEncoder: Encoder[RelatedEntities] = Encoders.bean(classOf[RelatedEntities])

      override def outputEncoder: Encoder[RelatedEntities] = Encoders.bean(classOf[RelatedEntities])
    }

  val scholixAggregator: Aggregator[(String, Scholix), Scholix, Scholix] =
    new Aggregator[(String, Scholix), Scholix, Scholix] with Serializable {
      override def zero: Scholix = null

      def scholix_complete(s: Scholix): Boolean = {
        if (s == null || s.getIdentifier == null) {
          false
        } else if (s.getSource == null || s.getTarget == null) {
          false
        } else if (s.getLinkprovider == null || s.getLinkprovider.isEmpty)
          false
        else
          true
      }

      override def reduce(b: Scholix, a: (String, Scholix)): Scholix = {
        if (scholix_complete(b)) b else a._2
      }

      override def merge(b1: Scholix, b2: Scholix): Scholix = {
        if (scholix_complete(b1)) b1 else b2
      }

      override def finish(reduction: Scholix): Scholix = reduction

      override def bufferEncoder: Encoder[Scholix] = Encoders.kryo[Scholix]

      override def outputEncoder: Encoder[Scholix] = Encoders.kryo[Scholix]
    }

  def createInverseScholixRelation(scholix: Scholix): Scholix = {
    val s = new Scholix
    s.setPublicationDate(scholix.getPublicationDate)
    s.setPublisher(scholix.getPublisher)
    s.setLinkprovider(scholix.getLinkprovider)
    s.setRelationship(inverseRelationShip(scholix.getRelationship))
    s.setSource(scholix.getTarget)
    s.setTarget(scholix.getSource)
    s.setIdentifier(
      DHPUtils.md5(
        s"${s.getSource.getIdentifier}::${s.getRelationship.getName}::${s.getTarget.getIdentifier}"
      )
    )
    s

  }

  def extractCollectedFrom(summary: ScholixResource): List[ScholixEntityId] = {
    if (summary.getCollectedFrom != null && !summary.getCollectedFrom.isEmpty) {
      val l: List[ScholixEntityId] = summary.getCollectedFrom.asScala.map { d =>
        new ScholixEntityId(d.getProvider.getName, d.getProvider.getIdentifiers)
      }(collection.breakOut)
      l
    } else List()
  }

  def extractCollectedFrom(summary: ScholixSummary): List[ScholixEntityId] = {
    if (summary.getDatasources != null && !summary.getDatasources.isEmpty) {
      val l: List[ScholixEntityId] = summary.getDatasources.asScala.map { d =>
        new ScholixEntityId(
          d.getDatasourceName,
          List(new ScholixIdentifier(d.getDatasourceId, "DNET Identifier", null)).asJava
        )
      }(collection.breakOut)
      l
    } else List()
  }

  def extractCollectedFrom(relation: Relation): List[ScholixEntityId] = {
    if (relation.getCollectedfrom != null && !relation.getCollectedfrom.isEmpty) {

      val l: List[ScholixEntityId] = relation.getCollectedfrom.asScala.map { c =>
        new ScholixEntityId(
          c.getValue,
          List(new ScholixIdentifier(c.getKey, DNET_IDENTIFIER_SCHEMA, null)).asJava
        )
      }.toList
      l
    } else List()
  }

  def generateCompleteScholix(scholix: Scholix, target: ScholixSummary): Scholix = {
    val s = new Scholix
    s.setPublicationDate(scholix.getPublicationDate)
    s.setPublisher(scholix.getPublisher)
    s.setLinkprovider(scholix.getLinkprovider)
    s.setRelationship(scholix.getRelationship)
    s.setSource(scholix.getSource)
    s.setTarget(generateScholixResourceFromSummary(target))
    s.setIdentifier(
      DHPUtils.md5(
        s"${s.getSource.getIdentifier}::${s.getRelationship.getName}::${s.getTarget.getIdentifier}"
      )
    )
    s
  }

  def generateCompleteScholix(scholix: Scholix, target: ScholixResource): Scholix = {
    val s = new Scholix
    s.setPublicationDate(scholix.getPublicationDate)
    s.setPublisher(scholix.getPublisher)
    s.setLinkprovider(scholix.getLinkprovider)
    s.setRelationship(scholix.getRelationship)
    s.setSource(scholix.getSource)
    s.setTarget(target)
    s.setIdentifier(
      DHPUtils.md5(
        s"${s.getSource.getIdentifier}::${s.getRelationship.getName}::${s.getTarget.getIdentifier}"
      )
    )
    s
  }

  def generateScholixResourceFromSummary(summaryObject: ScholixSummary): ScholixResource = {
    val r = new ScholixResource
    r.setIdentifier(summaryObject.getLocalIdentifier)
    r.setDnetIdentifier(summaryObject.getId)

    r.setObjectType(summaryObject.getTypology.toString)
    r.setObjectSubType(summaryObject.getSubType)

    if (summaryObject.getTitle != null && !summaryObject.getTitle.isEmpty)
      r.setTitle(summaryObject.getTitle.get(0))

    if (summaryObject.getAuthor != null && !summaryObject.getAuthor.isEmpty) {
      val l: List[ScholixEntityId] =
        summaryObject.getAuthor.asScala.map(a => new ScholixEntityId(a, null)).take(100).toList
      if (l.nonEmpty)
        r.setCreator(l.asJava)
    }

    if (summaryObject.getDate != null && !summaryObject.getDate.isEmpty)
      r.setPublicationDate(summaryObject.getDate.get(0))
    if (summaryObject.getPublisher != null && !summaryObject.getPublisher.isEmpty) {
      val plist: List[ScholixEntityId] =
        summaryObject.getPublisher.asScala.map(p => new ScholixEntityId(p, null)).take(100).toList

      if (plist.nonEmpty)
        r.setPublisher(plist.asJava)
    }

    if (summaryObject.getDatasources != null && !summaryObject.getDatasources.isEmpty) {

      val l: List[ScholixCollectedFrom] = summaryObject.getDatasources.asScala
        .map(c =>
          new ScholixCollectedFrom(
            new ScholixEntityId(
              c.getDatasourceName,
              List(new ScholixIdentifier(c.getDatasourceId, DNET_IDENTIFIER_SCHEMA, null)).asJava
            ),
            "collected",
            "complete"
          )
        )
        .take(100)
        .toList

      if (l.nonEmpty)
        r.setCollectedFrom(l.asJava)

    }
    r
  }

//  def scholixFromSource(relation: Relation, source: ScholixResource): Scholix = {
//    if (relation == null || source == null)
//      return null
//    val s = new Scholix
//    var l: List[ScholixEntityId] = extractCollectedFrom(relation)
//    if (l.isEmpty)
//      l = extractCollectedFrom(source)
//    if (l.isEmpty)
//      return null
//    s.setLinkprovider(l.asJava)
//    var d = extractRelationDate(relation)
//    if (d == null)
//      d = source.getPublicationDate
//
//    s.setPublicationDate(d)
//
//    if (source.getPublisher != null && !source.getPublisher.isEmpty) {
//      s.setPublisher(source.getPublisher)
//    }
//
//    val semanticRelation = relations.getOrElse(relation.getRelClass.toLowerCase, null)
//    if (semanticRelation == null)
//      return null
//    s.setRelationship(
//      new ScholixRelationship(semanticRelation.original, "datacite", semanticRelation.inverse)
//    )
//    s.setSource(source)
//
//    s
//  }

  def scholixFromSource(relation: Relation, source: ScholixResource): Scholix = {

    if (relation == null || source == null)
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

    if (source.getPublisher != null && !source.getPublisher.isEmpty) {
      source.getPublisher
      val l: List[ScholixEntityId] = source.getPublisher.asScala.toList
      if (l.nonEmpty)
        s.setPublisher(l.asJava)
    }

    val semanticRelation = relations.getOrElse(relation.getRelClass.toLowerCase, null)
    if (semanticRelation == null)
      return null
    s.setRelationship(
      new ScholixRelationship(semanticRelation.original, "datacite", semanticRelation.inverse)
    )
    s.setSource(source)

    s
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

  def resultToSummary(r: Result): ScholixSummary = {
    val s = new ScholixSummary
    s.setId(r.getId)
    if (r.getPid == null || r.getPid.isEmpty)
      return null

    val persistentIdentifiers: List[ScholixIdentifier] = extractTypedIdentifierFromInstance(r)
    if (persistentIdentifiers.isEmpty)
      return null
    s.setLocalIdentifier(persistentIdentifiers.asJava)
    if (r.isInstanceOf[Publication])
      s.setTypology(Typology.publication)
    else
      s.setTypology(Typology.dataset)

    s.setSubType(r.getInstance().get(0).getInstancetype.getClassname)

    if (r.getTitle != null && r.getTitle.asScala.nonEmpty) {
      val titles: List[String] = r.getTitle.asScala.map(t => t.getValue).toList
      if (titles.nonEmpty)
        s.setTitle(titles.asJava)
      else
        return null
    }

    if (r.getAuthor != null && !r.getAuthor.isEmpty) {
      val authors: List[String] = r.getAuthor.asScala.map(a => a.getFullname).toList
      if (authors.nonEmpty)
        s.setAuthor(authors.asJava)
    }
    if (r.getInstance() != null) {
      val dt: List[String] = r
        .getInstance()
        .asScala
        .filter(i => i.getDateofacceptance != null)
        .map(i => i.getDateofacceptance.getValue)
        .toList
      if (dt.nonEmpty)
        s.setDate(dt.distinct.asJava)
    }
    if (r.getDescription != null && !r.getDescription.isEmpty) {
      val d = r.getDescription.asScala.find(f => f != null && f.getValue != null)
      if (d.isDefined)
        s.setDescription(d.get.getValue)
    }

    if (r.getSubject != null && !r.getSubject.isEmpty) {
      val subjects: List[SchemeValue] = r.getSubject.asScala
        .map(s => new SchemeValue(s.getQualifier.getClassname, s.getValue))
        .toList
      if (subjects.nonEmpty)
        s.setSubject(subjects.asJava)
    }

    if (r.getPublisher != null)
      s.setPublisher(List(r.getPublisher.getValue).asJava)

    if (r.getCollectedfrom != null && !r.getCollectedfrom.isEmpty) {
      val cf: List[CollectedFromType] = r.getCollectedfrom.asScala
        .map(c => new CollectedFromType(c.getValue, c.getKey, "complete"))
        .toList
      if (cf.nonEmpty)
        s.setDatasources(cf.distinct.asJava)
    }

    s.setRelatedDatasets(0)
    s.setRelatedPublications(0)
    s.setRelatedUnknown(0)

    s
  }

}
