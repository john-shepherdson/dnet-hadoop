package eu.dnetlib.dhp.oa.dedup

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.oa.dedup.dsl.{Clustering, Deduper}
import eu.dnetlib.dhp.oa.dedup.model.BlockStats
import eu.dnetlib.dhp.utils.ISLookupClientFactory
import eu.dnetlib.enabling.is.lookup.rmi.{ISLookUpException, ISLookUpService}
import eu.dnetlib.pace.model.{RowDataOrderingComparator, SparkDedupConfig}
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.DataTypes
import org.dom4j.DocumentException
import org.slf4j.LoggerFactory
import org.xml.sax.SAXException

import java.io.IOException
import java.util.stream.Collectors

object DSLExample {
  private val log = LoggerFactory.getLogger(classOf[DSLExample])

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val parser = new ArgumentApplicationParser(
      IOUtils
        .toString(classOf[DSLExample].getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/createBlockStats_parameters.json"))
    )
    parser.parseArgument(args)
    val conf = new SparkConf
    new DSLExample(parser, AbstractSparkAction.getSparkSession(conf)).run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")))
  }
}

class DSLExample(parser: ArgumentApplicationParser, spark: SparkSession) extends AbstractSparkAction(parser, spark) {

  def computeComparisons(blockSize: Long, slidingWindowSize: Long): Long =
    if (slidingWindowSize >= blockSize) (slidingWindowSize * (slidingWindowSize - 1)) / 2
    else (blockSize - slidingWindowSize + 1) * (slidingWindowSize * (slidingWindowSize - 1)) / 2

  @throws[DocumentException]
  @throws[IOException]
  @throws[ISLookUpException]
  @throws[SAXException]
  override def run(isLookUpService: ISLookUpService): Unit = {
// read oozie parameters
    val graphBasePath = parser.get("graphBasePath")
    val isLookUpUrl = parser.get("isLookUpUrl")
    val actionSetId = parser.get("actionSetId")
    val workingPath = parser.get("workingPath")
    val numPartitions : Int = Option(parser.get("numPartitions")).map(_.toInt).getOrElse(AbstractSparkAction.NUM_PARTITIONS)

    DSLExample.log.info("graphBasePath: '{}'", graphBasePath)
    DSLExample.log.info("isLookUpUrl:   '{}'", isLookUpUrl)
    DSLExample.log.info("actionSetId:   '{}'", actionSetId)
    DSLExample.log.info("workingPath:   '{}'", workingPath)
    // for each dedup configuration
    import scala.collection.JavaConversions._
    for (dedupConf <- getConfigurations(isLookUpService, actionSetId).subList(0, 1)) {
      val subEntity = dedupConf.getWf.getSubEntityValue
      DSLExample.log.info("Creating blockstats for: '{}'", subEntity)
      val outputPath = DedupUtility.createBlockStatsPath(workingPath, actionSetId, subEntity)
      AbstractSparkAction.removeOutputDir(spark, outputPath)

      val sparkConfig = SparkDedupConfig(dedupConf, numPartitions)

      val inputDF = spark.read
        .textFile(DedupUtility.createEntityPath(graphBasePath, subEntity))
        .transform(sparkConfig.modelExtractor)
      val simRels = inputDF
        .transform(sparkConfig.generateClusters)
        .filter(functions.size(new Column("block")).geq(new Literal(1, DataTypes.IntegerType)))

      val deduper = Deduper(inputDF.schema)
        .withClustering( Clustering("sortedngrampairs"),
          Clustering("sortedngrampairs", Seq("legalname"), Map("max" -> 2, "ngramLen" -> 3)),
          Clustering("suffixprefix", Seq("legalname"), Map("max" -> 1, "len" -> 3)),
          Clustering("urlclustering", Seq("websiteurl")),
          Clustering("keywordsclustering", Seq("fields"),  Map("max" -> 2, "windowSize" -> 4))
        )

      simRels
        .map[BlockStats](
          (b:Row) => {
            val documents = b.getList(1)
            val mapDocuments = documents.stream
              .sorted(new RowDataOrderingComparator(sparkConfig.orderingFieldPosition))
              .limit(dedupConf.getWf.getQueueMaxSize)
              .collect(Collectors.toList)
            new BlockStats(
              b.getString(0),
              mapDocuments.size.toLong,
              computeComparisons(mapDocuments.size.toLong, dedupConf.getWf.getSlidingWindowSize.toLong)
            )

          })(Encoders.bean[BlockStats](classOf[BlockStats]))
        .write
        .mode(SaveMode.Overwrite)
        .save(outputPath)
    }
  }
}
