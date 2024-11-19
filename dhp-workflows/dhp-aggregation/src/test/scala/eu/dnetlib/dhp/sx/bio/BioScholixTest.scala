package eu.dnetlib.dhp.sx.bio

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import eu.dnetlib.dhp.aggregation.AbstractVocabularyTest
import eu.dnetlib.dhp.schema.oaf.utils.PidType
import eu.dnetlib.dhp.schema.oaf.{Oaf, Publication, Relation, Result}
import eu.dnetlib.dhp.sx.bio.BioDBToOAF.ScholixResolved
import eu.dnetlib.dhp.sx.bio.ebi.SparkCreatePubmedDump
import eu.dnetlib.dhp.sx.bio.pubmed._
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s.jackson.JsonMethods.parse
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.junit.jupiter.MockitoExtension
import org.slf4j.LoggerFactory

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.util.regex.Pattern
import java.util.zip.GZIPInputStream
import javax.xml.stream.XMLInputFactory
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

@ExtendWith(Array(classOf[MockitoExtension]))
class BioScholixTest extends AbstractVocabularyTest {

  val mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  @BeforeEach
  def setUp(): Unit = {

    super.setUpVocabulary()
  }

  class BufferedReaderIterator(reader: BufferedReader) extends Iterator[String] {
    override def hasNext() = reader.ready
    override def next() = reader.readLine()
  }

  object GzFileIterator {

    def apply(is: InputStream, encoding: String) = {
      new BufferedReaderIterator(
        new BufferedReader(new InputStreamReader(new GZIPInputStream(is), encoding))
      )
    }
  }

  @Test
  def testPid(): Unit = {
    val pids = List(
      "0000000163025705",
      "000000018494732X",
      "0000000308873343",
      "0000000335964515",
      "0000000333457333",
      "0000000335964515",
      "0000000302921949",

      "http://orcid.org/0000-0001-8567-3543",
      "http://orcid.org/0000-0001-7868-8528",
      "0000-0001-9189-1440",
      "0000-0003-3727-9247",
      "0000-0001-7246-1058",
      "000000033962389X",
      "0000000330371470",
      "0000000171236123",
      "0000000272569752",
      "0000000293231371",
      "http://orcid.org/0000-0003-3345-7333",
      "0000000340145688",
      "http://orcid.org/0000-0003-4894-1689"
    )

    pids.foreach(pid => {
      val pidCleaned = new PMIdentifier(pid, "ORCID").getPid
      // assert pid is in the format of ORCID
      println(pidCleaned)
      assertTrue(pidCleaned.matches("[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X]"))
    })
  }

  def extractAffiliation(s: String): List[String] = {
    val regex: String = "<Affiliation>(.*)<\\/Affiliation>"
    val pattern = Pattern.compile(regex, Pattern.MULTILINE)
    val matcher = pattern.matcher(s)
    val l: mutable.ListBuffer[String] = mutable.ListBuffer()
    while (matcher.find()) {
      l += matcher.group(1)
    }
    l.toList
  }

  case class AuthorPID(pidType: String, pid: String) {}

  def extractAuthorIdentifier(s: String): List[AuthorPID] = {
    val regex: String = "<Identifier Source=\"(.*)\">(.*)<\\/Identifier>"
    val pattern = Pattern.compile(regex, Pattern.MULTILINE)
    val matcher = pattern.matcher(s)
    val l: mutable.ListBuffer[AuthorPID] = mutable.ListBuffer()
    while (matcher.find()) {
      l += AuthorPID(pidType = matcher.group(1), pid = matcher.group(2))
    }
    l.toList
  }

  @Test
  def testParsingPubmed2(): Unit = {
    val mapper = new ObjectMapper()
    val xml = IOUtils.toString(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/bio/single_pubmed.xml"))
    val parser = new PMParser2()
    val article = parser.parse(xml)

//    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(article))

    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(PubMedToOaf.convert(article, vocabularies)))

  }

  @Test
  def testEBIData() = {
    val inputFactory = XMLInputFactory.newInstance
    val xml = inputFactory.createXMLEventReader(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/bio/pubmed.xml"))
    new PMParser(xml).foreach(s => println(mapper.writeValueAsString(s)))
  }

  @Test
  def testPubmedToOaf(): Unit = {
    assertNotNull(vocabularies)
    assertTrue(vocabularies.vocabularyExists("dnet:publication_resource"))
    val records: String = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/bio/pubmed_dump"))
      .mkString
    val r: List[Oaf] = records.linesWithSeparators
      .map(l => l.stripLineEnd)
      .toList
      .map(s => mapper.readValue(s, classOf[PMArticle]))
      .map(a => PubMedToOaf.convert(a, vocabularies))
    assertEquals(10, r.size)
    assertTrue(
      r.map(p => p.asInstanceOf[Result])
        .flatMap(p => p.getInstance().asScala.map(i => i.getInstancetype.getClassid))
        .exists(p => "0037".equalsIgnoreCase(p))
    )
    println(mapper.writeValueAsString(r.head))

  }

  private def checkPMArticle(article: PMArticle): Unit = {
    assertNotNull(article.getPmid)
    assertNotNull(article.getTitle)
    assertNotNull(article.getAuthors)
    article.getAuthors.asScala.foreach { a =>
      assertNotNull(a)
      assertNotNull(a.getFullName)
    }

  }

  @Test
  def testParsingPubmedXML(): Unit = {
    val inputFactory = XMLInputFactory.newInstance

    val xml = inputFactory.createXMLEventReader(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/bio/pubmed.xml"))

    val parser = new PMParser(xml)
    parser.foreach(checkPMArticle)
  }

  private def checkPubmedPublication(o: Oaf): Unit = {
    assertTrue(o.isInstanceOf[Publication])
    val p: Publication = o.asInstanceOf[Publication]
    assertNotNull(p.getId)
    assertNotNull(p.getTitle)
    p.getTitle.asScala.foreach(t => assertNotNull(t.getValue))
    p.getAuthor.asScala.foreach(a => assertNotNull(a.getFullname))
    assertNotNull(p.getInstance())
    p.getInstance().asScala.foreach { i =>
      assertNotNull(i.getCollectedfrom)
      assertNotNull(i.getPid)
      assertNotNull(i.getInstancetype)
    }
    assertNotNull(p.getOriginalId)
    p.getOriginalId.asScala.foreach(oId => assertNotNull(oId))

    val hasPMC = p
      .getInstance()
      .asScala
      .exists(i => i.getPid.asScala.exists(pid => pid.getQualifier.getClassid.equalsIgnoreCase(PidType.pmc.toString)))

    if (hasPMC) {
      assertTrue(p.getOriginalId.asScala.exists(oId => oId.startsWith("od_______267::")))
    }
  }

  def testPubmedSplitting(): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("test").master("local").getOrCreate()
    new SparkCreatePubmedDump("", Array.empty, LoggerFactory.getLogger(getClass))
      .createPubmedDump(spark, "/home/sandro/Downloads/pubmed", "/home/sandro/Downloads/pubmed_mapped", vocabularies)

  }

  @Test
  def testPubmedOriginalID(): Unit = {
    val article: PMArticle = new PMArticle

    article.setPmid("1234")

    article.setTitle("a Title")

    // VERIFY PUBLICATION IS NOT NULL
    article.getPublicationTypes.add(new PMSubject("article", null, null))
    var publication = PubMedToOaf.convert(article, vocabularies).asInstanceOf[Publication]
    assertNotNull(publication)
    assertEquals("50|pmid________::81dc9bdb52d04dc20036dbd8313ed055", publication.getId)

    // VERIFY PUBLICATION ID DOES NOT CHANGE ALSO IF SETTING PMC IDENTIFIER
    article.setPmcId("PMC1517292")
    publication = PubMedToOaf.convert(article, vocabularies).asInstanceOf[Publication]
    assertNotNull(publication)
    assertEquals("50|pmid________::81dc9bdb52d04dc20036dbd8313ed055", publication.getId)

    // VERIFY ORIGINAL ID GENERATE IN OLD WAY USING PMC IDENTIFIER EXISTS

    val oldOpenaireID = "od_______267::0000072375bc0e68fa09d4e6b7658248"

    val hasOldOpenAIREID = publication.getOriginalId.asScala.exists(o => o.equalsIgnoreCase(oldOpenaireID))

    assertTrue(hasOldOpenAIREID)
  }

  @Test
  def testPubmedMapping(): Unit = {

    val inputFactory = XMLInputFactory.newInstance
    val xml = inputFactory.createXMLEventReader(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/bio/pubmed.xml"))

    val parser = new PMParser(xml)
    val results = ListBuffer[Oaf]()
    parser.foreach(x => results += PubMedToOaf.convert(x, vocabularies))

    results.foreach(checkPubmedPublication)

  }

  @Test
  def testPDBToOAF(): Unit = {

    assertNotNull(vocabularies)
    assertTrue(vocabularies.vocabularyExists("dnet:publication_resource"))
    val records: String = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/bio/pdb_dump"))
      .mkString
    records.linesWithSeparators.map(l => l.stripLineEnd).foreach(s => assertTrue(s.nonEmpty))

    val result: List[Oaf] =
      records.linesWithSeparators.map(l => l.stripLineEnd).toList.flatMap(o => BioDBToOAF.pdbTOOaf(o))

    assertTrue(result.nonEmpty)
    result.foreach(r => assertNotNull(r))

    println(result.count(o => o.isInstanceOf[Relation]))
    println(mapper.writeValueAsString(result.head))

  }

  @Test
  def testUNIprotToOAF(): Unit = {

    assertNotNull(vocabularies)
    assertTrue(vocabularies.vocabularyExists("dnet:publication_resource"))

    val records: String = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/bio/uniprot_dump"))
      .mkString
    records.linesWithSeparators.map(l => l.stripLineEnd).foreach(s => assertTrue(s.nonEmpty))

    val result: List[Oaf] =
      records.linesWithSeparators.map(l => l.stripLineEnd).toList.flatMap(o => BioDBToOAF.uniprotToOAF(o))

    assertTrue(result.nonEmpty)
    result.foreach(r => assertNotNull(r))

    println(result.count(o => o.isInstanceOf[Relation]))
    println(mapper.writeValueAsString(result.head))

  }

  case class EBILinks(
    relType: String,
    date: String,
    title: String,
    pmid: String,
    targetPid: String,
    targetPidType: String
  ) {}

  def parse_ebi_links(input: String): List[EBILinks] = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json = parse(input)
    val pmid = (json \ "publication" \ "pmid").extract[String]
    for {
      JObject(link)                                       <- json \\ "Link"
      JField("Target", JObject(target))                   <- link
      JField("RelationshipType", JObject(relType))        <- link
      JField("Name", JString(relation))                   <- relType
      JField("PublicationDate", JString(publicationDate)) <- link
      JField("Title", JString(title))                     <- target
      JField("Identifier", JObject(identifier))           <- target
      JField("IDScheme", JString(idScheme))               <- identifier
      JField("ID", JString(id))                           <- identifier

    } yield EBILinks(relation, publicationDate, title, pmid, id, idScheme)
  }

  @Test
  def testCrossrefLinksToOAF(): Unit = {

    val records: String = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/bio/crossref_links"))
      .mkString
    records.linesWithSeparators.map(l => l.stripLineEnd).foreach(s => assertTrue(s.nonEmpty))

    val result: List[Oaf] =
      records.linesWithSeparators.map(l => l.stripLineEnd).map(s => BioDBToOAF.crossrefLinksToOaf(s)).toList

    assertNotNull(result)
    assertTrue(result.nonEmpty)

    println(mapper.writeValueAsString(result.head))

  }

  @Test
  def testEBILinksToOAF(): Unit = {
    val iterator = GzFileIterator(
      getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/bio/ebi_links.gz"),
      "UTF-8"
    )
    val data = iterator.next()

    val res = BioDBToOAF
      .parse_ebi_links(BioDBToOAF.extractEBILinksFromDump(data).links)
      .filter(BioDBToOAF.EBITargetLinksFilter)
      .flatMap(BioDBToOAF.convertEBILinksToOaf)
    print(res.length)

    println(mapper.writeValueAsString(res.head))

  }

  @Test
  def scholixResolvedToOAF(): Unit = {

    val records: String = Source
      .fromInputStream(
        getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/bio/scholix_resolved")
      )
      .mkString
    records.linesWithSeparators.map(l => l.stripLineEnd).foreach(s => assertTrue(s.nonEmpty))

    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

    val l: List[ScholixResolved] = records.linesWithSeparators
      .map(l => l.stripLineEnd)
      .map { input =>
        lazy val json = parse(input)
        json.extract[ScholixResolved]
      }
      .toList

    val result: List[Oaf] = l.map(s => BioDBToOAF.scholixResolvedToOAF(s))

    assertTrue(result.nonEmpty)
  }

}
