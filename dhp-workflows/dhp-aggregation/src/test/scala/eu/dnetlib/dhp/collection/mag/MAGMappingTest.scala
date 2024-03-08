package eu.dnetlib.dhp.collection.mag

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.junit.jupiter.api.Test

class MAGMappingTest {

  val mapper = new ObjectMapper()

  @Test
  def mappingTest(): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Test")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val magDS = spark.read.load("/home/sandro/Downloads/mag").as[MAGPaper].where(col("journalId").isNotNull)

    val paper = magDS.first()

    print(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(MagUtility.convertMAGtoOAF(paper)))

  }

}
