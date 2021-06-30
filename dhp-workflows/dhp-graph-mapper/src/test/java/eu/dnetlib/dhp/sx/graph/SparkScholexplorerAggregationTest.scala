package eu.dnetlib.dhp.sx.graph

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import eu.dnetlib.dhp.schema.scholexplorer.DLIPublication
import eu.dnetlib.dhp.sx.graph.ebi.EBIAggregator
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.io.Source

class SparkScholexplorerAggregationTest {


  @Test
  def testFunderRelationshipsMapping(): Unit = {
    val publications = Source.fromInputStream(getClass.getResourceAsStream("publication.json")).mkString

    var s: List[DLIPublication] = List[DLIPublication]()

    val m: ObjectMapper = new ObjectMapper()

    m.enable(SerializationFeature.INDENT_OUTPUT)

    for (line <- publications.lines) {
      s = m.readValue(line, classOf[DLIPublication]) :: s


    }


    implicit val pubEncoder: Encoder[DLIPublication] = Encoders.kryo[DLIPublication]
    val spark: SparkSession = SparkSession.builder().appName("Test").master("local[*]").config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()


    val ds: Dataset[DLIPublication] = spark.createDataset(spark.sparkContext.parallelize(s)).as[DLIPublication]

    val unique = ds.map(d => (d.getId, d))(Encoders.tuple(Encoders.STRING, pubEncoder))
      .groupByKey(_._1)(Encoders.STRING)
      .agg(EBIAggregator.getDLIPublicationAggregator().toColumn)
      .map(p => p._2)

    val uniquePubs: DLIPublication = unique.first()

    s.foreach(pp => assertFalse(pp.getAuthor.isEmpty))


    assertNotNull(uniquePubs.getAuthor)
    assertFalse(uniquePubs.getAuthor.isEmpty)


  }

}
