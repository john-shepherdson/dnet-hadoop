package eu.dnetlib.dhp.sx.graph.scholix

import eu.dnetlib.dhp.schema.sx.scholix.ScholixResource
import eu.dnetlib.dhp.sx.graph.SparkCreateScholexplorerDump
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.junit.jupiter.api.Test
import org.objenesis.strategy.StdInstantiatorStrategy

class ScholixGenerationTest {

  @Test
  def generateScholix(): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
    val app = new SparkCreateScholexplorerDump(null, null, null)
//   app.generateScholixResource("/home/sandro/Downloads/scholix_sample/", "/home/sandro/Downloads/scholix/", spark)
//    app.generateBidirectionalRelations(
//      "/home/sandro/Downloads/scholix_sample/",
//      "/home/sandro/Downloads/scholix/",
//      spark
//    )
    app.generateScholix("/home/sandro/Downloads/scholix/", spark)

  }
}
