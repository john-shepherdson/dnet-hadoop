package eu.dnetlib.dhp.datacite

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import eu.dnetlib.dhp.aggregation.AbstractVocabularyTest
import eu.dnetlib.dhp.schema.oaf.Oaf
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.junit.jupiter.MockitoExtension
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.{Files, Path}
import java.text.SimpleDateFormat
import java.util.Locale
import scala.io.Source

@ExtendWith(Array(classOf[MockitoExtension]))
class DataciteToOAFTest extends AbstractVocabularyTest {

  private var workingDir: Path = null
  val log: Logger = LoggerFactory.getLogger(getClass)

  @BeforeEach
  def setUp(): Unit = {

    workingDir = Files.createTempDirectory(getClass.getSimpleName)
    super.setUpVocabulary()
  }

  @AfterEach
  def tearDown(): Unit = {
    FileUtils.deleteDirectory(workingDir.toFile)
  }

  @Test
  def testDateMapping: Unit = {
    val inputDate = "2021-07-14T11:52:54+0000"
    val ISO8601FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ", Locale.US)
    val dt = ISO8601FORMAT.parse(inputDate)
    println(dt.getTime)

  }

  @Test
  def testConvert(): Unit = {

    val path = getClass.getResource("/eu/dnetlib/dhp/actionmanager/datacite/dataset").getPath

    val conf = new SparkConf()
    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]
    val instance = new GenerateDataciteDatasetSpark(null, null, log)
    val targetPath = s"$workingDir/result"

    instance.generateDataciteDataset(path, exportLinks = true, vocabularies, targetPath, spark)

    import spark.implicits._

    val nativeSize = spark.read.load(path).count()

    assertEquals(100, nativeSize)

    spark.read.load(targetPath).printSchema();


    val result: Dataset[Oaf] = spark.read.load(targetPath).as[Oaf]

    result
      .map(s => s.getClass.getSimpleName)
      .groupBy(col("value").alias("class"))
      .agg(count("value").alias("Total"))
      .show(false)

    val t = spark.read.load(targetPath).count()

    assertTrue(t > 0)

    spark.stop()

  }

  @Test
  def testMapping(): Unit = {
    val record = Source
      .fromInputStream(
        getClass.getResourceAsStream("/eu/dnetlib/dhp/actionmanager/datacite/record.json")
      )
      .mkString

    val mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
    val res: List[Oaf] = DataciteToOAFTransformation.generateOAF(record, 0L, 0L, vocabularies, true)

    res.foreach(r => {
      println(mapper.writeValueAsString(r))
      println("----------------------------")

    })

  }

}
