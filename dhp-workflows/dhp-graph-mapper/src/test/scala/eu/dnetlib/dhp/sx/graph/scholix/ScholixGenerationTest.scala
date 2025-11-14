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

    val basePath = "/home/sandro/Develop/openaire/kubernetes/openaire-local-envs/sharedfs/spark/tmp/prod_provision/graph"

    app.generateBidirectionalRelations(s"$basePath/10_graph_blacklisted/",
      s"$basePath/scholix/workingPath", spark)
    app.generateScholixResource(s"$basePath/10_graph_blacklisted/",
      s"$basePath/scholix/workingPath", spark)
    app.generateFlatScholix(s"$basePath/scholix/workingPath",s"$basePath/scholix/final", spark)
    app.generateSummary(s"$basePath/scholix/workingPath",s"$basePath/scholix/final", spark)




  }
}
