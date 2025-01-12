package eu.dnetlib.dhp.doiboost.crossref

import com.fasterxml.jackson.databind.SerializationFeature
import eu.dnetlib.dhp.aggregation.AbstractVocabularyTest
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf._
import eu.dnetlib.dhp.utils.DHPUtils
import eu.dnetlib.doiboost.crossref.Crossref2Oaf
import org.json4s
import com.fasterxml.jackson.databind.ObjectMapper
import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s.{DefaultFormats, JValue}
import org.json4s.jackson.JsonMethods
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.junit.jupiter.MockitoExtension
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.Files
import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.matching.Regex

@ExtendWith(Array(classOf[MockitoExtension]))
class CrossrefMappingTest extends AbstractVocabularyTest {

  val logger: Logger = LoggerFactory.getLogger(Crossref2Oaf.getClass)
  val mapper = new ObjectMapper()

  @BeforeEach
  def setUp(): Unit = {
    super.setUpVocabulary()
  }

  @Test
  def testMissingAuthorParser(): Unit = {
    val json: String = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/doiboost/crossref/s41567-022-01757-y.json"))
      .mkString
    val result = Crossref2Oaf.convert(json, vocabularies)
    result
      .filter(o => o.isInstanceOf[Publication])
      .map(p => p.asInstanceOf[Publication])
      .foreach(p => assertTrue(p.getAuthor.size() > 0))
  }

  @Test
  def testFunderRelationshipsMapping(): Unit = {
    val template = Source
      .fromInputStream(
        getClass.getResourceAsStream("/eu/dnetlib/doiboost/crossref/article_funder_template.json")
      )
      .mkString
    val funder_doi = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/doiboost/crossref/funder_doi"))
      .mkString
    val funder_name = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/doiboost/crossref/funder_doi"))
      .mkString

    for (line <- funder_doi.linesWithSeparators.map(l => l.stripLineEnd)) {
      val json = template.replace("%s", line)
      val resultList: List[Oaf] = Crossref2Oaf.convert(json, vocabularies)
      assertTrue(resultList.nonEmpty)
      checkRelation(resultList)
    }
    for (line <- funder_name.linesWithSeparators.map(l => l.stripLineEnd)) {
      val json = template.replace("%s", line)
      val resultList: List[Oaf] = Crossref2Oaf.convert(json, vocabularies)
      assertTrue(resultList.nonEmpty)
      checkRelation(resultList)
    }
  }

  def checkRelation(generatedOAF: List[Oaf]): Unit = {

    val rels: List[Relation] =
      generatedOAF.filter(p => p.isInstanceOf[Relation]).asInstanceOf[List[Relation]]
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
  def testSum(): Unit = {
    val from: Long = 1613135645000L
    val delta: Long = 1000000L

    println(s"updating from value: $from  -> ${from + delta}")

  }

  @Test
  def crossrefIssueDateTest(): Unit = {
    val json =
      Source.fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/doiboost/crossref/issue_date.json")).mkString
    assertNotNull(json)
    assertFalse(json.isEmpty)
    val resultList: List[Oaf] = Crossref2Oaf.convert(json, vocabularies)
    assertTrue(resultList.nonEmpty)

    val items = resultList.filter(p => p.isInstanceOf[Result])

    println(mapper.writeValueAsString(items.head))
  }

  @Test
  def testOrcidID(): Unit = {
    val json = Source
      .fromInputStream(
        getClass.getResourceAsStream("/eu/dnetlib/doiboost/crossref/orcid_data.json")
      )
      .mkString

    assertNotNull(json)
    assertFalse(json.isEmpty)

    val resultList: List[Oaf] = Crossref2Oaf.convert(json, vocabularies)

    assertTrue(resultList.nonEmpty)

    val items = resultList.filter(p => p.isInstanceOf[Result])

    items.foreach(p => println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(p)))

  }

  private def parseJson(input: String): JValue = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = JsonMethods.parse(input)

    json
  }

  @Test
  def testCitationRelations(): Unit = {
    val json = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/doiboost/crossref/publication_license_embargo.json"))
      .mkString

    assertNotNull(json)
    assertFalse(json.isEmpty)

    val result: List[Oaf] = Crossref2Oaf.convert(json, vocabularies)

    assertTrue(result.nonEmpty)

    val j = parseJson(json)

    val doisReference: List[String] = for {
      JObject(reference_json)          <- j \ "reference"
      JField("DOI", JString(doi_json)) <- reference_json
    } yield doi_json

    val relationList: List[Relation] = result
      .filter(s => s.isInstanceOf[Relation])
      .map(r => r.asInstanceOf[Relation])
      .filter(r => r.getSubRelType.equalsIgnoreCase(ModelConstants.CITATION))

    assertNotNull(relationList)
    assertFalse(relationList.isEmpty)

    assertEquals(doisReference.size, relationList.size)

    relationList.foreach(p => println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(p)))
  }

  @Test
  def testEmptyTitle(): Unit = {
    val json = Source
      .fromInputStream(
        getClass.getResourceAsStream("/eu/dnetlib/doiboost/crossref/empty_title.json")
      )
      .mkString

    assertNotNull(json)
    assertFalse(json.isEmpty);

    val resultList: List[Oaf] = Crossref2Oaf.convert(json, vocabularies)

    assertTrue(resultList.nonEmpty)

    val items = resultList.filter(p => p.isInstanceOf[Result])

    items.foreach(p => println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(p)))

  }

  @Test
  def testPeerReviewed(): Unit = {
    val json = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/doiboost/crossref/prwTest.json"))
      .mkString

    assertNotNull(json)
    assertFalse(json.isEmpty);

    val resultList: List[Oaf] = Crossref2Oaf.convert(json, vocabularies)

    assertTrue(resultList.nonEmpty)

    val items = resultList.filter(p => p.isInstanceOf[Result])

    items.foreach(p => logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(p)))

  }

  def extractECAward(award: String): String = {
    val awardECRegex: Regex = "[0-9]{4,9}".r
    if (awardECRegex.findAllIn(award).hasNext)
      return awardECRegex.findAllIn(award).max
    null
  }

  @Test
  def extractECTest(): Unit = {
    val s = "FP7/2007-2013"
    val awardExtracted = extractECAward(s)
    println(awardExtracted)

    println(DHPUtils.md5(awardExtracted))

  }

  @Test
  def testJournalRelation(): Unit = {
    val json = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/doiboost/crossref/awardTest.json"))
      .mkString
    assertNotNull(json)

    assertFalse(json.isEmpty)

    val resultList: List[Oaf] = Crossref2Oaf.convert(json, vocabularies)

    assertTrue(resultList.nonEmpty)
    val rels: List[Relation] =
      resultList.filter(p => p.isInstanceOf[Relation]).map(r => r.asInstanceOf[Relation])

    rels.foreach(s => logger.info(s.getTarget))
    assertEquals(rels.size, 6)

  }

  @Test
  def testConvertBookFromCrossRef2Oaf(): Unit = {
    val json = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/doiboost/crossref/book.json"))
      .mkString
    assertNotNull(json)

    assertFalse(json.isEmpty);

    val resultList: List[Oaf] = Crossref2Oaf.convert(json, vocabularies)

    assertTrue(resultList.nonEmpty)

    val items = resultList.filter(p => p.isInstanceOf[Result])

    assert(items.nonEmpty)
    assert(items.size == 1)
    val result: Result = items.head.asInstanceOf[Result]
    assertNotNull(result)

    logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));

    assertNotNull(result.getDataInfo, "Datainfo test not null Failed");
    assertNotNull(
      result.getDataInfo.getProvenanceaction,
      "DataInfo/Provenance test not null Failed"
    );
    assertFalse(
      result.getDataInfo.getProvenanceaction.getClassid.isEmpty,
      "DataInfo/Provenance/classId test not null Failed"
    );
    assertFalse(
      result.getDataInfo.getProvenanceaction.getClassname.isEmpty,
      "DataInfo/Provenance/className test not null Failed"
    );
    assertFalse(
      result.getDataInfo.getProvenanceaction.getSchemeid.isEmpty,
      "DataInfo/Provenance/SchemeId test not null Failed"
    );
    assertFalse(
      result.getDataInfo.getProvenanceaction.getSchemename.isEmpty,
      "DataInfo/Provenance/SchemeName test not null Failed"
    );

    assertNotNull(result.getCollectedfrom, "CollectedFrom test not null Failed");
    assertFalse(result.getCollectedfrom.isEmpty);

    val collectedFromList = result.getCollectedfrom.asScala
    assert(
      collectedFromList.exists(c => c.getKey.equalsIgnoreCase("10|openaire____::081b82f96300b6a6e3d282bad31cb6e2")),
      "Wrong collected from assertion"
    )

    assert(
      collectedFromList.exists(c => c.getValue.equalsIgnoreCase("crossref")),
      "Wrong collected from assertion"
    )

    val relevantDates = result.getRelevantdate.asScala

    assert(
      relevantDates.exists(d => d.getQualifier.getClassid.equalsIgnoreCase("created")),
      "Missing relevant date of type created"
    )
    assert(
      relevantDates.exists(d => d.getQualifier.getClassid.equalsIgnoreCase("published-online")),
      "Missing relevant date of type published-online"
    )
    assert(
      relevantDates.exists(d => d.getQualifier.getClassid.equalsIgnoreCase("published-print")),
      "Missing relevant date of type published-print"
    )
    val rels = resultList.filter(p => p.isInstanceOf[Relation])
    assert(rels.isEmpty)
  }

  @Test
  def testConvertPreprintFromCrossRef2Oaf(): Unit = {
    val json = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/doiboost/crossref/preprint.json"))
      .mkString
    assertNotNull(json)

    assertFalse(json.isEmpty);

    val resultList: List[Oaf] = Crossref2Oaf.convert(json, vocabularies)

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
      "DataInfo/Provenance test not null Failed"
    );
    assertFalse(
      result.getDataInfo.getProvenanceaction.getClassid.isEmpty,
      "DataInfo/Provenance/classId test not null Failed"
    );
    assertFalse(
      result.getDataInfo.getProvenanceaction.getClassname.isEmpty,
      "DataInfo/Provenance/className test not null Failed"
    );
    assertFalse(
      result.getDataInfo.getProvenanceaction.getSchemeid.isEmpty,
      "DataInfo/Provenance/SchemeId test not null Failed"
    );
    assertFalse(
      result.getDataInfo.getProvenanceaction.getSchemename.isEmpty,
      "DataInfo/Provenance/SchemeName test not null Failed"
    );

    assertNotNull(result.getCollectedfrom, "CollectedFrom test not null Failed");
    assertFalse(result.getCollectedfrom.isEmpty);

    val collectedFromList = result.getCollectedfrom.asScala
    assert(
      collectedFromList.exists(c => c.getKey.equalsIgnoreCase("10|openaire____::081b82f96300b6a6e3d282bad31cb6e2")),
      "Wrong collected from assertion"
    )

    assert(
      collectedFromList.exists(c => c.getValue.equalsIgnoreCase("crossref")),
      "Wrong collected from assertion"
    )

    val relevantDates = result.getRelevantdate.asScala

    assert(
      relevantDates.exists(d => d.getQualifier.getClassid.equalsIgnoreCase("created")),
      "Missing relevant date of type created"
    )
    assert(
      relevantDates.exists(d => d.getQualifier.getClassid.equalsIgnoreCase("available")),
      "Missing relevant date of type available"
    )
    assert(
      relevantDates.exists(d => d.getQualifier.getClassid.equalsIgnoreCase("accepted")),
      "Missing relevant date of type accepted"
    )
    assert(
      relevantDates.exists(d => d.getQualifier.getClassid.equalsIgnoreCase("published-online")),
      "Missing relevant date of type published-online"
    )
    assert(
      relevantDates.exists(d => d.getQualifier.getClassid.equalsIgnoreCase("published-print")),
      "Missing relevant date of type published-print"
    )
    val rels = resultList.filter(p => p.isInstanceOf[Relation])
    assert(rels.isEmpty)
  }

  @Test
  def testConvertDatasetFromCrossRef2Oaf(): Unit = {
    val json = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/doiboost/crossref/dataset.json"))
      .mkString
    assertNotNull(json)

    assertFalse(json.isEmpty);

    val resultList: List[Oaf] = Crossref2Oaf.convert(json, vocabularies)

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
      "DataInfo/Provenance test not null Failed"
    );
    assertFalse(
      result.getDataInfo.getProvenanceaction.getClassid.isEmpty,
      "DataInfo/Provenance/classId test not null Failed"
    );
    assertFalse(
      result.getDataInfo.getProvenanceaction.getClassname.isEmpty,
      "DataInfo/Provenance/className test not null Failed"
    );
    assertFalse(
      result.getDataInfo.getProvenanceaction.getSchemeid.isEmpty,
      "DataInfo/Provenance/SchemeId test not null Failed"
    );
    assertFalse(
      result.getDataInfo.getProvenanceaction.getSchemename.isEmpty,
      "DataInfo/Provenance/SchemeName test not null Failed"
    );

    assertNotNull(result.getCollectedfrom, "CollectedFrom test not null Failed");
    assertFalse(result.getCollectedfrom.isEmpty);
  }

  @Test
  def testConvertArticleFromCrossRef2Oaf(): Unit = {
    val json = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/doiboost/crossref/article.json"))
      .mkString
    assertNotNull(json)

    assertFalse(json.isEmpty);

    val resultList: List[Oaf] = Crossref2Oaf.convert(json, vocabularies)

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
      "DataInfo/Provenance test not null Failed"
    );
    assertFalse(
      result.getDataInfo.getProvenanceaction.getClassid.isEmpty,
      "DataInfo/Provenance/classId test not null Failed"
    );
    assertFalse(
      result.getDataInfo.getProvenanceaction.getClassname.isEmpty,
      "DataInfo/Provenance/className test not null Failed"
    );
    assertFalse(
      result.getDataInfo.getProvenanceaction.getSchemeid.isEmpty,
      "DataInfo/Provenance/SchemeId test not null Failed"
    );
    assertFalse(
      result.getDataInfo.getProvenanceaction.getSchemename.isEmpty,
      "DataInfo/Provenance/SchemeName test not null Failed"
    );

    assertNotNull(result.getCollectedfrom, "CollectedFrom test not null Failed");
    assertFalse(result.getCollectedfrom.isEmpty);

    val collectedFromList = result.getCollectedfrom.asScala
    assert(
      collectedFromList.exists(c => c.getKey.equalsIgnoreCase("10|openaire____::081b82f96300b6a6e3d282bad31cb6e2")),
      "Wrong collected from assertion"
    )

    assert(
      collectedFromList.exists(c => c.getValue.equalsIgnoreCase("crossref")),
      "Wrong collected from assertion"
    )

    val relevantDates = result.getRelevantdate.asScala

    assert(
      relevantDates.exists(d => d.getQualifier.getClassid.equalsIgnoreCase("created")),
      "Missing relevant date of type created"
    )

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

  @Test
  def testConvertFromCrossRef2OafIssue(): Unit = {
    val json = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/doiboost/crossref/article_nojournal.json"))
      .mkString
    assertNotNull(json)

    assertFalse(json.isEmpty);

    val resultList: List[Oaf] = Crossref2Oaf.convert(json, vocabularies)

    assertTrue(resultList.nonEmpty)

    val items = resultList.filter(p => p.isInstanceOf[Publication])

    assert(items.nonEmpty)
    assert(items.size == 1)
    val pub: Publication = items.head.asInstanceOf[Publication]

    assertNotNull(pub.getJournal.getIssnPrinted)
    assertNotNull(pub.getJournal.getIssnOnline)
    assertNotNull(pub.getJournal.getName)

  }

  @Test
  def testSetDateOfAcceptanceCrossRef2Oaf(): Unit = {

    val json = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/doiboost/crossref/dump_file.json"))
      .mkString
    assertNotNull(json)

    assertFalse(json.isEmpty);

    val resultList: List[Oaf] = Crossref2Oaf.convert(json, vocabularies)

    assertTrue(resultList.nonEmpty)

    val items = resultList.filter(p => p.isInstanceOf[Publication])

    assert(items.nonEmpty)
    assert(items.size == 1)
    val result: Result = items.head.asInstanceOf[Publication]
    assertNotNull(result)

    logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));
  }

  @Test
  def testNormalizeDOI(): Unit = {
    val template = Source
      .fromInputStream(
        getClass.getResourceAsStream("/eu/dnetlib/doiboost/crossref/article_funder_template.json")
      )
      .mkString
    val line: String =
      "\"funder\": [{\"name\": \"Wellcome Trust Masters Fellowship\",\"award\": [\"090633\"]}],"
    val json = template.replace("%s", line)
    val resultList: List[Oaf] = Crossref2Oaf.convert(json, vocabularies)
    assertTrue(resultList.nonEmpty)
    val items = resultList.filter(p => p.isInstanceOf[Publication])
    val result: Result = items.head.asInstanceOf[Publication]

    result.getPid.asScala.foreach(pid => assertTrue(pid.getQualifier.getClassid.equals("doi")))
    assertTrue(result.getPid.size() == 1)
    result.getPid.asScala.foreach(pid =>
      assertTrue(pid.getValue.equals("10.26850/1678-4618EQJ.v35.1.2010.p41-46".toLowerCase()))
    )

  }

  @Test
  def testNormalizeDOI2(): Unit = {
    val template = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/doiboost/crossref/article.json"))
      .mkString

    val resultList: List[Oaf] = Crossref2Oaf.convert(template, vocabularies)
    assertTrue(resultList.nonEmpty)
    val items = resultList.filter(p => p.isInstanceOf[Publication])
    val result: Result = items.head.asInstanceOf[Publication]

    result.getPid.asScala.foreach(pid => assertTrue(pid.getQualifier.getClassid.equals("doi")))
    assertTrue(result.getPid.size() == 1)
    result.getPid.asScala.foreach(pid =>
      assertTrue(pid.getValue.equals("10.26850/1678-4618EQJ.v35.1.2010.p41-46".toLowerCase()))
    )

  }

  @Test
  def testLicenseVorClosed(): Unit = {
    val json = Source
      .fromInputStream(
        getClass.getResourceAsStream("/eu/dnetlib/doiboost/crossref/publication_license_vor.json")
      )
      .mkString

    assertNotNull(json)
    assertFalse(json.isEmpty);

    val resultList: List[Oaf] = Crossref2Oaf.convert(json, vocabularies)

    assertTrue(resultList.nonEmpty)

    val item: Result = resultList.filter(p => p.isInstanceOf[Result]).head.asInstanceOf[Result]

    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(item))

    assertTrue(
      item.getInstance().asScala exists (i => i.getLicense.getValue.equals("https://www.springer.com/vor"))
    )
    assertTrue(
      item.getInstance().asScala exists (i => i.getAccessright.getClassid.equals("CLOSED"))
    )
    assertTrue(item.getInstance().asScala exists (i => i.getAccessright.getOpenAccessRoute == null))

  }

  @Test
  def testLicenseOpen(): Unit = {
    val json = Source
      .fromInputStream(
        getClass.getResourceAsStream("/eu/dnetlib/doiboost/crossref/publication_license_open.json")
      )
      .mkString

    assertNotNull(json)
    assertFalse(json.isEmpty);

    val resultList: List[Oaf] = Crossref2Oaf.convert(json, vocabularies)

    assertTrue(resultList.nonEmpty)

    val item: Result = resultList.filter(p => p.isInstanceOf[Result]).head.asInstanceOf[Result]

    assertTrue(
      item.getInstance().asScala exists (i =>
        i.getLicense.getValue.equals(
          "http://pubs.acs.org/page/policy/authorchoice_ccby_termsofuse.html"
        )
      )
    )
    assertTrue(item.getInstance().asScala exists (i => i.getAccessright.getClassid.equals("OPEN")))
    assertTrue(
      item.getInstance().asScala exists (i => i.getAccessright.getOpenAccessRoute == OpenAccessRoute.hybrid)
    )

    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(item))

  }

  @Test
  def testLicenseEmbargoOpen(): Unit = {
    val json = Source
      .fromInputStream(
        getClass.getResourceAsStream(
          "/eu/dnetlib/doiboost/crossref/publication_license_embargo_open.json"
        )
      )
      .mkString

    assertNotNull(json)
    assertFalse(json.isEmpty);

    val resultList: List[Oaf] = Crossref2Oaf.convert(json, vocabularies)

    assertTrue(resultList.nonEmpty)

    val item: Result = resultList.filter(p => p.isInstanceOf[Result]).head.asInstanceOf[Result]

    assertTrue(
      item.getInstance().asScala exists (i =>
        i.getLicense.getValue.equals(
          "https://academic.oup.com/journals/pages/open_access/funder_policies/chorus/standard_publication_model"
        )
      )
    )
    assertTrue(item.getInstance().asScala exists (i => i.getAccessright.getClassid.equals("OPEN")))
    assertTrue(
      item.getInstance().asScala exists (i => i.getAccessright.getOpenAccessRoute == OpenAccessRoute.hybrid)
    )
    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(item))

  }

  @Test
  def testLicenseEmbargo(): Unit = {
    val json = Source
      .fromInputStream(
        getClass.getResourceAsStream(
          "/eu/dnetlib/doiboost/crossref/publication_license_embargo.json"
        )
      )
      .mkString

    assertNotNull(json)
    assertFalse(json.isEmpty);

    val resultList: List[Oaf] = Crossref2Oaf.convert(json, vocabularies)

    assertTrue(resultList.nonEmpty)

    val item: Result = resultList.filter(p => p.isInstanceOf[Result]).head.asInstanceOf[Result]

    assertTrue(
      item.getInstance().asScala exists (i =>
        i.getLicense.getValue.equals(
          "https://academic.oup.com/journals/pages/open_access/funder_policies/chorus/standard_publication_model"
        )
      )
    )
    assertTrue(
      item.getInstance().asScala exists (i => i.getAccessright.getClassid.equals("EMBARGO"))
    )
    assertTrue(item.getInstance().asScala exists (i => i.getAccessright.getOpenAccessRoute == null))
    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(item))

  }

  @Test
  def testLicenseEmbargoDateTime(): Unit = {
    val json = Source
      .fromInputStream(
        getClass.getResourceAsStream(
          "/eu/dnetlib/doiboost/crossref/publication_license_embargo_datetime.json"
        )
      )
      .mkString

    assertNotNull(json)
    assertFalse(json.isEmpty);

    val resultList: List[Oaf] = Crossref2Oaf.convert(json, vocabularies)

    assertTrue(resultList.nonEmpty)

    val item: Result = resultList.filter(p => p.isInstanceOf[Result]).head.asInstanceOf[Result]

    assertTrue(
      item.getInstance().asScala exists (i =>
        i.getLicense.getValue.equals(
          "https://academic.oup.com/journals/pages/open_access/funder_policies/chorus/standard_publication_model"
        )
      )
    )
    assertTrue(
      item.getInstance().asScala exists (i => i.getAccessright.getClassid.equals("EMBARGO"))
    )
    assertTrue(item.getInstance().asScala exists (i => i.getAccessright.getOpenAccessRoute == null))

    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(item))

  }

  @Test
  def testMultipleURLs(): Unit = {
    val json = Source
      .fromInputStream(
        getClass.getResourceAsStream("/eu/dnetlib/doiboost/crossref/multiple_urls.json")
      )
      .mkString

    assertNotNull(json)
    assertFalse(json.isEmpty);

    val resultList: List[Oaf] = Crossref2Oaf.convert(json, vocabularies)

    assertTrue(resultList.nonEmpty)

    val item: Result = resultList.filter(p => p.isInstanceOf[Result]).head.asInstanceOf[Result]

    assertEquals(1, item.getInstance().size())
    assertEquals(1, item.getInstance().get(0).getUrl().size())
    assertEquals(
      "https://doi.org/10.1016/j.jas.2019.105013",
      item.getInstance().get(0).getUrl.get(0)
    )

  }

}
