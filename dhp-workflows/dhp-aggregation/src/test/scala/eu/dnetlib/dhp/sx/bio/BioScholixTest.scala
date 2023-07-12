package eu.dnetlib.dhp.sx.bio

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import eu.dnetlib.dhp.aggregation.AbstractVocabularyTest
import eu.dnetlib.dhp.schema.oaf.utils.PidType
import eu.dnetlib.dhp.schema.oaf.{Oaf, Publication, Relation, Result}
import eu.dnetlib.dhp.sx.bio.BioDBToOAF.ScholixResolved
import eu.dnetlib.dhp.sx.bio.pubmed.{PMArticle, PMParser, PMSubject, PubMedToOaf, PubmedParser}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s.jackson.JsonMethods.parse
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.junit.jupiter.MockitoExtension

import java.io.{BufferedReader, FileInputStream, InputStream, InputStreamReader}
import java.util.zip.GZIPInputStream
import scala.collection.JavaConverters._
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
  def testEBIData() = {
    val inputXML = getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/bio/pubmed.xml")


   // new PubmedParser(new GZIPInputStream(new FileInputStream("/Users/sandro/Downloads/pubmed23n1078.xml.gz")))
    new PMParser(new GZIPInputStream(new FileInputStream("/Users/sandro/Downloads/pubmed23n1078.xml.gz")))
    print("DONE")
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

//  @Test
//  def testParsingPubmedXML(): Unit = {
//    val xml = new XMLEventReader(
//      Source.fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/bio/pubmed.xml"))
//    )
//    val parser = new PMParser(xml)
//    parser.foreach(checkPMArticle)
//  }

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

//  @Test
//  def testPubmedMapping(): Unit = {
//
//    val xml = new XMLEventReader(
//      Source.fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/graph/bio/pubmed.xml"))
//    )
//    val parser = new PMParser(xml)
//    val results = ListBuffer[Oaf]()
//    parser.foreach(x => results += PubMedToOaf.convert(x, vocabularies))
//
//    results.foreach(checkPubmedPublication)
//
//  }

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
