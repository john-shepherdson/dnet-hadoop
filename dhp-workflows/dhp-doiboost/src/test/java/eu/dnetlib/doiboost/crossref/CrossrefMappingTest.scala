package eu.dnetlib.doiboost.crossref

import eu.dnetlib.dhp.schema.oaf._
import eu.dnetlib.dhp.utils.DHPUtils
import org.codehaus.jackson.map.{ObjectMapper, SerializationConfig}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.matching.Regex


class CrossrefMappingTest {

  val logger: Logger = LoggerFactory.getLogger(Crossref2Oaf.getClass)
  val mapper = new ObjectMapper()


  @Test
  def testFunderRelationshipsMapping(): Unit = {
    val template = Source.fromInputStream(getClass.getResourceAsStream("article_funder_template.json")).mkString
    val funder_doi = Source.fromInputStream(getClass.getResourceAsStream("funder_doi")).mkString
    val funder_name = Source.fromInputStream(getClass.getResourceAsStream("funder_doi")).mkString


    for (line <- funder_doi.lines) {
      val json = template.replace("%s", line)
      val resultList: List[Oaf] = Crossref2Oaf.convert(json)
      assertTrue(resultList.nonEmpty)
      checkRelation(resultList)
    }
    for (line <- funder_name.lines) {
      val json = template.replace("%s", line)
      val resultList: List[Oaf] = Crossref2Oaf.convert(json)
      assertTrue(resultList.nonEmpty)
      checkRelation(resultList)
    }
  }

  def checkRelation(generatedOAF: List[Oaf]): Unit = {

    val rels: List[Relation] = generatedOAF.filter(p => p.isInstanceOf[Relation]).asInstanceOf[List[Relation]]
    assertFalse(rels.isEmpty)
    rels.foreach(relation => {
      val relJson = mapper.writeValueAsString(relation)

      assertNotNull(relation.getSource, s"Source of relation null $relJson")
      assertNotNull(relation.getTarget, s"Target of relation null $relJson")
      assertFalse(relation.getTarget.isEmpty, s"Target is empty: $relJson")
      assertFalse(relation.getRelClass.isEmpty, s"RelClass is empty: $relJson")
      assertFalse(relation.getRelType.isEmpty, s"RelType is empty: $relJson")
      assertFalse(relation.getSubRelType.isEmpty, s"SubRelType is empty: $relJson")

    })

  }


  @Test
  def testPeerReviewed(): Unit = {
    val json = Source.fromInputStream(getClass.getResourceAsStream("prwTest.json")).mkString
    mapper.getSerializationConfig.enable(SerializationConfig.Feature.INDENT_OUTPUT)

    assertNotNull(json)
    assertFalse(json.isEmpty);

    val resultList: List[Oaf] = Crossref2Oaf.convert(json)

    assertTrue(resultList.nonEmpty)

    val items = resultList.filter(p => p.isInstanceOf[Result])


    items.foreach(p => logger.info(mapper.writeValueAsString(p)))




  }

  def extractECAward(award: String): String = {
    val awardECRegex: Regex = "[0-9]{4,9}".r
    if (awardECRegex.findAllIn(award).hasNext)
      return awardECRegex.findAllIn(award).max
    null
  }


  @Test
  def extractECTest(): Unit = {
    val s =  "FP7/2007-2013"
    val awardExtracted = extractECAward(s)
    println(awardExtracted)

    println(DHPUtils.md5(awardExtracted))


  }

  @Test
  def testJournalRelation(): Unit = {
    val json = Source.fromInputStream(getClass.getResourceAsStream("awardTest.json")).mkString
    assertNotNull(json)

    assertFalse(json.isEmpty)

    val resultList: List[Oaf] = Crossref2Oaf.convert(json)

    assertTrue(resultList.nonEmpty)
    val rels:List[Relation] = resultList.filter(p => p.isInstanceOf[Relation]).map(r=> r.asInstanceOf[Relation])



    rels.foreach(s => logger.info(s.getTarget))
    assertEquals(rels.size, 3 )


  }


  @Test
  def testConvertBookFromCrossRef2Oaf(): Unit = {
    val json = Source.fromInputStream(getClass.getResourceAsStream("book.json")).mkString
    assertNotNull(json)

    assertFalse(json.isEmpty);

    val resultList: List[Oaf] = Crossref2Oaf.convert(json)

    assertTrue(resultList.nonEmpty)

    val items = resultList.filter(p => p.isInstanceOf[Result])

    assert(items.nonEmpty)
    assert(items.size == 1)
    val result: Result = items.head.asInstanceOf[Result]
    assertNotNull(result)

    logger.info(mapper.writeValueAsString(result));

    assertNotNull(result.getDataInfo, "Datainfo test not null Failed");
    assertNotNull(
      result.getDataInfo.getProvenanceaction,
      "DataInfo/Provenance test not null Failed");
    assertFalse(
      result.getDataInfo.getProvenanceaction.getClassid.isEmpty,
      "DataInfo/Provenance/classId test not null Failed");
    assertFalse(
      result.getDataInfo.getProvenanceaction.getClassname.isEmpty,
      "DataInfo/Provenance/className test not null Failed");
    assertFalse(
      result.getDataInfo.getProvenanceaction.getSchemeid.isEmpty,
      "DataInfo/Provenance/SchemeId test not null Failed");
    assertFalse(
      result.getDataInfo.getProvenanceaction.getSchemename.isEmpty,
      "DataInfo/Provenance/SchemeName test not null Failed");

    assertNotNull(result.getCollectedfrom, "CollectedFrom test not null Failed");
    assertFalse(result.getCollectedfrom.isEmpty);

    val collectedFromList = result.getCollectedfrom.asScala
    assert(collectedFromList.exists(c => c.getKey.equalsIgnoreCase("10|openaire____::081b82f96300b6a6e3d282bad31cb6e2")), "Wrong collected from assertion")

    assert(collectedFromList.exists(c => c.getValue.equalsIgnoreCase("crossref")), "Wrong collected from assertion")


    val relevantDates = result.getRelevantdate.asScala

    assert(relevantDates.exists(d => d.getQualifier.getClassid.equalsIgnoreCase("created")), "Missing relevant date of type created")
    assert(relevantDates.exists(d => d.getQualifier.getClassid.equalsIgnoreCase("published-online")), "Missing relevant date of type published-online")
    assert(relevantDates.exists(d => d.getQualifier.getClassid.equalsIgnoreCase("published-print")), "Missing relevant date of type published-print")
    val rels = resultList.filter(p => p.isInstanceOf[Relation])
    assert(rels.isEmpty)
  }


  @Test
  def testConvertPreprintFromCrossRef2Oaf(): Unit = {
    val json = Source.fromInputStream(getClass.getResourceAsStream("preprint.json")).mkString
    assertNotNull(json)

    assertFalse(json.isEmpty);

    val resultList: List[Oaf] = Crossref2Oaf.convert(json)

    assertTrue(resultList.nonEmpty)

    val items = resultList.filter(p => p.isInstanceOf[Publication])

    assert(items.nonEmpty)
    assert(items.size == 1)
    val result: Result = items.head.asInstanceOf[Publication]
    assertNotNull(result)

    logger.info(mapper.writeValueAsString(result));

    assertNotNull(result.getDataInfo, "Datainfo test not null Failed");
    assertNotNull(
      result.getDataInfo.getProvenanceaction,
      "DataInfo/Provenance test not null Failed");
    assertFalse(
      result.getDataInfo.getProvenanceaction.getClassid.isEmpty,
      "DataInfo/Provenance/classId test not null Failed");
    assertFalse(
      result.getDataInfo.getProvenanceaction.getClassname.isEmpty,
      "DataInfo/Provenance/className test not null Failed");
    assertFalse(
      result.getDataInfo.getProvenanceaction.getSchemeid.isEmpty,
      "DataInfo/Provenance/SchemeId test not null Failed");
    assertFalse(
      result.getDataInfo.getProvenanceaction.getSchemename.isEmpty,
      "DataInfo/Provenance/SchemeName test not null Failed");

    assertNotNull(result.getCollectedfrom, "CollectedFrom test not null Failed");
    assertFalse(result.getCollectedfrom.isEmpty);

    val collectedFromList = result.getCollectedfrom.asScala
    assert(collectedFromList.exists(c => c.getKey.equalsIgnoreCase("10|openaire____::081b82f96300b6a6e3d282bad31cb6e2")), "Wrong collected from assertion")

    assert(collectedFromList.exists(c => c.getValue.equalsIgnoreCase("crossref")), "Wrong collected from assertion")


    val relevantDates = result.getRelevantdate.asScala

    assert(relevantDates.exists(d => d.getQualifier.getClassid.equalsIgnoreCase("created")), "Missing relevant date of type created")
    assert(relevantDates.exists(d => d.getQualifier.getClassid.equalsIgnoreCase("available")), "Missing relevant date of type available")
    assert(relevantDates.exists(d => d.getQualifier.getClassid.equalsIgnoreCase("accepted")), "Missing relevant date of type accepted")
    assert(relevantDates.exists(d => d.getQualifier.getClassid.equalsIgnoreCase("published-online")), "Missing relevant date of type published-online")
    assert(relevantDates.exists(d => d.getQualifier.getClassid.equalsIgnoreCase("published-print")), "Missing relevant date of type published-print")
    val rels = resultList.filter(p => p.isInstanceOf[Relation])
    assert(rels.isEmpty)
  }


  @Test
  def testConvertDatasetFromCrossRef2Oaf(): Unit = {
    val json = Source.fromInputStream(getClass.getResourceAsStream("dataset.json")).mkString
    assertNotNull(json)

    assertFalse(json.isEmpty);

    val resultList: List[Oaf] = Crossref2Oaf.convert(json)

    assertTrue(resultList.nonEmpty)

    val items = resultList.filter(p => p.isInstanceOf[Dataset])

    assert(items.nonEmpty)
    assert(items.size == 1)
    val result: Result = items.head.asInstanceOf[Dataset]
    assertNotNull(result)

    logger.info(mapper.writeValueAsString(result));

    assertNotNull(result.getDataInfo, "Datainfo test not null Failed");
    assertNotNull(
      result.getDataInfo.getProvenanceaction,
      "DataInfo/Provenance test not null Failed");
    assertFalse(
      result.getDataInfo.getProvenanceaction.getClassid.isEmpty,
      "DataInfo/Provenance/classId test not null Failed");
    assertFalse(
      result.getDataInfo.getProvenanceaction.getClassname.isEmpty,
      "DataInfo/Provenance/className test not null Failed");
    assertFalse(
      result.getDataInfo.getProvenanceaction.getSchemeid.isEmpty,
      "DataInfo/Provenance/SchemeId test not null Failed");
    assertFalse(
      result.getDataInfo.getProvenanceaction.getSchemename.isEmpty,
      "DataInfo/Provenance/SchemeName test not null Failed");

    assertNotNull(result.getCollectedfrom, "CollectedFrom test not null Failed");
    assertFalse(result.getCollectedfrom.isEmpty);
  }

  @Test
  def testConvertArticleFromCrossRef2Oaf(): Unit = {
    val json = Source.fromInputStream(getClass.getResourceAsStream("article.json")).mkString
    assertNotNull(json)

    assertFalse(json.isEmpty);

    val resultList: List[Oaf] = Crossref2Oaf.convert(json)

    assertTrue(resultList.nonEmpty)

    val items = resultList.filter(p => p.isInstanceOf[Publication])

    assert(items.nonEmpty)
    assert(items.size == 1)
    val result: Result = items.head.asInstanceOf[Publication]
    assertNotNull(result)

    logger.info(mapper.writeValueAsString(result));

    assertNotNull(result.getDataInfo, "Datainfo test not null Failed");
    assertNotNull(
      result.getDataInfo.getProvenanceaction,
      "DataInfo/Provenance test not null Failed");
    assertFalse(
      result.getDataInfo.getProvenanceaction.getClassid.isEmpty,
      "DataInfo/Provenance/classId test not null Failed");
    assertFalse(
      result.getDataInfo.getProvenanceaction.getClassname.isEmpty,
      "DataInfo/Provenance/className test not null Failed");
    assertFalse(
      result.getDataInfo.getProvenanceaction.getSchemeid.isEmpty,
      "DataInfo/Provenance/SchemeId test not null Failed");
    assertFalse(
      result.getDataInfo.getProvenanceaction.getSchemename.isEmpty,
      "DataInfo/Provenance/SchemeName test not null Failed");

    assertNotNull(result.getCollectedfrom, "CollectedFrom test not null Failed");
    assertFalse(result.getCollectedfrom.isEmpty);

    val collectedFromList = result.getCollectedfrom.asScala
    assert(collectedFromList.exists(c => c.getKey.equalsIgnoreCase("10|openaire____::081b82f96300b6a6e3d282bad31cb6e2")), "Wrong collected from assertion")

    assert(collectedFromList.exists(c => c.getValue.equalsIgnoreCase("crossref")), "Wrong collected from assertion")


    val relevantDates = result.getRelevantdate.asScala

    assert(relevantDates.exists(d => d.getQualifier.getClassid.equalsIgnoreCase("created")), "Missing relevant date of type created")

    val rels = resultList.filter(p => p.isInstanceOf[Relation]).asInstanceOf[List[Relation]]
    assertFalse(rels.isEmpty)
    rels.foreach(relation => {
      assertNotNull(relation)
      assertFalse(relation.getSource.isEmpty)
      assertFalse(relation.getTarget.isEmpty)
      assertFalse(relation.getRelClass.isEmpty)
      assertFalse(relation.getRelType.isEmpty)
      assertFalse(relation.getSubRelType.isEmpty)

    })


  }

}
