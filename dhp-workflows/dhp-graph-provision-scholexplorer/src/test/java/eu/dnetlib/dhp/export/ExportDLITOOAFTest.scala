package eu.dnetlib.dhp.export

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import eu.dnetlib.dhp.schema.oaf.Relation
import eu.dnetlib.dhp.schema.scholexplorer.{DLIDataset, DLIPublication}

import org.codehaus.jackson.map.{ObjectMapper, SerializationConfig}
import org.junit.jupiter.api.Test

import scala.io.Source

class ExportDLITOOAFTest {

  val mapper = new ObjectMapper()

  @Test
  def testDate():Unit = {
    println(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")))

  }


  @Test
  def testMappingRele():Unit = {

    val r:Relation = new Relation
    r.setSource("60|fbff1d424e045eecf24151a5fe3aa738")
    r.setTarget("50|dedup_wf_001::ec409f09e63347d4e834087fe1483877")

    val r1 =DLIToOAF.convertDLIRelation(r)
    println(r1.getSource, r1.getTarget)

  }

  @Test
  def testPublicationMapping():Unit = {

    mapper.getSerializationConfig.enable(SerializationConfig.Feature.INDENT_OUTPUT)
    val json = Source.fromInputStream(getClass.getResourceAsStream("publication.json")).mkString


    val oaf =DLIToOAF.convertDLIPublicationToOAF(mapper.readValue(json, classOf[DLIPublication]))

    println(mapper.writeValueAsString(oaf))


  }


  @Test
  def testExternalReferenceMapping():Unit = {

    mapper.getSerializationConfig.enable(SerializationConfig.Feature.INDENT_OUTPUT)
    val json = Source.fromInputStream(getClass.getResourceAsStream("dataset.json")).mkString


    val oaf =DLIToOAF.convertDLIDatasetToExternalReference(mapper.readValue(json, classOf[DLIDataset]))

    println(oaf)


  }







  @Test
  def testRelationMapping():Unit = {

    mapper.getSerializationConfig.enable(SerializationConfig.Feature.INDENT_OUTPUT)
    val json = Source.fromInputStream(getClass.getResourceAsStream("relation.json")).mkString


    val oaf =mapper.readValue(json, classOf[Relation])

    println(mapper.writeValueAsString(oaf))


  }

}
