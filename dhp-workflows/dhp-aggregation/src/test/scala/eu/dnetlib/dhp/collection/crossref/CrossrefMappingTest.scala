package eu.dnetlib.dhp.collection.crossref

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.aggregation.AbstractVocabularyTest
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

@ExtendWith(Array(classOf[MockitoExtension]))
class CrossrefMappingTest extends AbstractVocabularyTest {

  val logger: Logger = LoggerFactory.getLogger(Crossref2Oaf.getClass)
  val mapper = new ObjectMapper()

  @BeforeEach
  def setUp(): Unit = {
    super.setUpVocabulary()
  }

  @Test
  def testMapping(): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("TransformCrossref").getOrCreate()

    val s = new SparkMapDumpIntoOAF(null, null, null)
    import spark.implicits._

    s.transformCrossref(
      spark,
      sourcePath = "/home/sandro/Downloads/crossref",
      targetPath = "/home/sandro/Downloads/crossref_transformed",
      vocabularies = vocabularies
    )

    print(spark.read.text("/home/sandro/Downloads/crossref_transformed").count)
  }

}
