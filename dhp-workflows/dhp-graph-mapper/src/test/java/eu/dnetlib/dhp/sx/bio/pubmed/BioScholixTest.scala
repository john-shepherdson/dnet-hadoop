package eu.dnetlib.dhp.sx.bio.pubmed

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import eu.dnetlib.dhp.schema.oaf.{Oaf, Relation, Result}
import eu.dnetlib.dhp.sx.bio.BioDBToOAF
import eu.dnetlib.dhp.sx.ebi.SparkEBILinksToOaf
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.junit.jupiter.MockitoExtension

import java.io.{BufferedReader, FileInputStream, InputStream, InputStreamReader}
import java.util.zip.GZIPInputStream
import scala.collection.JavaConverters._
import scala.io.Source
import scala.xml.pull.XMLEventReader
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s.jackson.JsonMethods.parse

@ExtendWith(Array(classOf[MockitoExtension]))
class BioScholixTest extends AbstractVocabularyTest{


  @BeforeEach
  def setUp() :Unit = {

    super.setUpVocabulary()
  }

  class BufferedReaderIterator(reader: BufferedReader) extends Iterator[String] {
    override def hasNext() = reader.ready
    override def next() = reader.readLine()
  }

  object GzFileIterator {
    def apply(is: InputStream, encoding: String) = {
      new BufferedReaderIterator(
        new BufferedReader(
          new InputStreamReader(
            new GZIPInputStream(
              is), encoding)))
    }
  }


  @Test
  def testEBIData() = {

    val mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
    val inputXML = Source.fromInputStream(getClass.getResourceAsStream("pubmed.xml")).mkString
    val xml = new XMLEventReader(Source.fromBytes(inputXML.getBytes()))
    new PMParser(xml).foreach(s =>println(mapper.writeValueAsString(s)))
  }


  @Test
  def testPubmedToOaf(): Unit = {
    assertNotNull(vocabularies)
    assertTrue(vocabularies.vocabularyExists("dnet:publication_resource"))
    val mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)

    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false)
    val records:String =Source.fromInputStream(getClass.getResourceAsStream("pubmed_dump")).mkString
    val r:List[Oaf] = records.lines.toList.map(s=>mapper.readValue(s, classOf[PMArticle])).map(a => PubMedToOaf.convert(a, vocabularies))
    assertEquals(10, r.size)
    assertTrue(r.map(p => p.asInstanceOf[Result]).flatMap(p => p.getInstance().asScala.map(i => i.getInstancetype.getClassid)).exists(p => "0037".equalsIgnoreCase(p)))
    println(mapper.writeValueAsString(r.head))
  }


  @Test
  def testPDBToOAF():Unit = {

    assertNotNull(vocabularies)
    assertTrue(vocabularies.vocabularyExists("dnet:publication_resource"))

    val mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false)
    val records:String =Source.fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/bio/pdb_dump")).mkString
    records.lines.foreach(s => assertTrue(s.nonEmpty))

    val result:List[Oaf]=  records.lines.toList.flatMap(o => BioDBToOAF.pdbTOOaf(o))



    assertTrue(result.nonEmpty)
    result.foreach(r => assertNotNull(r))

    println(result.count(o => o.isInstanceOf[Relation]))
    println(mapper.writeValueAsString(result.head))

  }


  @Test
  def testUNIprotToOAF():Unit = {

    assertNotNull(vocabularies)
    assertTrue(vocabularies.vocabularyExists("dnet:publication_resource"))

    val mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false)
    val records:String =Source.fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/bio/uniprot_dump")).mkString
    records.lines.foreach(s => assertTrue(s.nonEmpty))

    val result:List[Oaf]=  records.lines.toList.flatMap(o => BioDBToOAF.uniprotToOAF(o))



    assertTrue(result.nonEmpty)
    result.foreach(r => assertNotNull(r))

    println(result.count(o => o.isInstanceOf[Relation]))
    println(mapper.writeValueAsString(result.head))

  }

  case class EBILinks(relType:String, date:String, title:String, pmid:String, targetPid:String, targetPidType:String) {}

  def parse_ebi_links(input:String):List[EBILinks] ={
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json = parse(input)
    val pmid = (json \ "publication" \"pmid").extract[String]
    for {
      JObject(link) <- json \\ "Link"
      JField("Target",JObject(target)) <- link
      JField("RelationshipType",JObject(relType)) <- link
      JField("Name", JString(relation)) <- relType
      JField("PublicationDate",JString(publicationDate)) <- link
      JField("Title", JString(title)) <- target
      JField("Identifier",JObject(identifier)) <- target
      JField("IDScheme", JString(idScheme)) <- identifier
      JField("ID", JString(id)) <- identifier

    } yield EBILinks(relation, publicationDate, title, pmid, id, idScheme)
  }


  @Test
  def testEBILinksToOAF():Unit = {
    val iterator = GzFileIterator(getClass.getResourceAsStream("/eu/dnetlib/dhp/sx/bio/ebi_links.gz"), "UTF-8")
    val data = iterator.next()

    val mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false)

    val res = BioDBToOAF.parse_ebi_links(BioDBToOAF.extractEBILinksFromDump(data).links).filter(BioDBToOAF.EBITargetLinksFilter).flatMap(BioDBToOAF.convertEBILinksToOaf)
    print(res.length)

  }

}
