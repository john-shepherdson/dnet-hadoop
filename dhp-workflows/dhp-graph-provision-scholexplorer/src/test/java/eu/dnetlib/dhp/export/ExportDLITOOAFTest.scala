package eu.dnetlib.dhp.export

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import eu.dnetlib.dhp.schema.oaf.Relation
import eu.dnetlib.dhp.schema.scholexplorer.{DLIDataset, DLIPublication, DLIRelation}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
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


    val oaf =DLIToOAF.convertDLIRelation(mapper.readValue(json, classOf[DLIRelation]))

    println(mapper.writeValueAsString(oaf))


  }

}
