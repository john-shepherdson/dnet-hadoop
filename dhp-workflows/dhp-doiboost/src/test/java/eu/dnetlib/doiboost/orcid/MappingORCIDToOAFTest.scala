package eu.dnetlib.doiboost.orcid

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.schema.oaf.Publication
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.Path
import scala.io.Source

class MappingORCIDToOAFTest {
  val logger: Logger = LoggerFactory.getLogger(ORCIDToOAF.getClass)
  val mapper = new ObjectMapper()

  @Test
  def testExtractData():Unit ={
    val json = Source.fromInputStream(getClass.getResourceAsStream("dataOutput")).mkString
    assertNotNull(json)
    assertFalse(json.isEmpty)
    json.lines.foreach(s => {
      assertNotNull(ORCIDToOAF.extractValueFromInputString(s))
    })
  }

  @Test
  def testOAFConvert(@TempDir testDir: Path):Unit ={
    val sourcePath:String = getClass.getResource("/eu/dnetlib/doiboost/orcid/datasets").getPath
    val targetPath: String =s"${testDir.toString}/output/orcidPublication"
    val workingPath =s"${testDir.toString}/wp/"

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .master("local[*]").getOrCreate()
    implicit val mapEncoderPubs: Encoder[Publication] = Encoders.kryo[Publication]
    import spark.implicits._

    SparkConvertORCIDToOAF.run( spark,sourcePath, workingPath, targetPath)

    val mapper = new ObjectMapper()



    val oA = spark.read.load(s"$workingPath/orcidworksWithAuthor").as[ORCIDItem].count()



    val p: Dataset[Publication] = spark.read.load(targetPath).as[Publication]

    assertTrue(oA == p.count())
    println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(p.first()))


  }







}
