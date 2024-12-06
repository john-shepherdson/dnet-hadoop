package eu.dnetlib.dhp.sx.graph.scholix

import eu.dnetlib.dhp.schema.sx.scholix.ScholixResource
import eu.dnetlib.dhp.sx.graph.SparkCreateScholexplorerDump
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.junit.jupiter.api.{Disabled, Test}
import org.objenesis.strategy.StdInstantiatorStrategy

class ScholixGenerationTest {

  @Test
  @Disabled
  def generateScholix(): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val app = new SparkCreateScholexplorerDump(null, null, null)

    val basePath = "/Users/sandro/Downloads"
    app.generateScholixResource(s"$basePath/scholix_sample/", s"$basePath/scholix/", spark)
    app.generateBidirectionalRelations(
      s"$basePath/scholix_sample/",
      s"$basePath/scholix/",
      spark
    )
    app.generateFlatScholix(s"$basePath/scholix/", spark)

  }
}
