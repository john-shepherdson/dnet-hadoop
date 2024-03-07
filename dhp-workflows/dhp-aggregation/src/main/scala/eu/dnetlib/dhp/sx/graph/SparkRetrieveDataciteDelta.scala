package eu.dnetlib.dhp.sx.graph

import eu.dnetlib.dhp.application.AbstractScalaApplication
import eu.dnetlib.dhp.collection.CollectionUtils.fixRelations
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup
import eu.dnetlib.dhp.datacite.{DataciteToOAFTransformation, DataciteType}
import eu.dnetlib.dhp.schema.oaf.{Oaf, Relation, Result}
import eu.dnetlib.dhp.schema.sx.scholix.{Scholix, ScholixResource}
import eu.dnetlib.dhp.schema.sx.summary.ScholixSummary
import eu.dnetlib.dhp.sx.graph.scholix.ScholixUtils
import eu.dnetlib.dhp.utils.{DHPUtils, ISLookupClientFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import java.text.SimpleDateFormat

class SparkRetrieveDataciteDelta(propertyPath: String, args: Array[String], log: Logger)
    extends AbstractScalaApplication(propertyPath, args, log: Logger) {

  val ISO_DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ssZ"
  val simpleFormatter = new SimpleDateFormat(ISO_DATE_PATTERN)

  val SCHOLIX_RESOURCE_PATH_NAME = "scholixResource"
  val DATACITE_OAF_PATH_NAME = "dataciteOAFUpdate"
  val PID_MAP_PATH_NAME = "pidMap"
  val RESOLVED_REL_PATH_NAME = "resolvedRelation"
  val SCHOLIX_PATH_NAME = "scholix"

  def scholixResourcePath(workingPath: String) = s"$workingPath/$SCHOLIX_RESOURCE_PATH_NAME"
  def dataciteOAFPath(workingPath: String) = s"$workingPath/$DATACITE_OAF_PATH_NAME"
  def pidMapPath(workingPath: String) = s"$workingPath/$PID_MAP_PATH_NAME"
  def resolvedRelationPath(workingPath: String) = s"$workingPath/$RESOLVED_REL_PATH_NAME"
  def scholixPath(workingPath: String) = s"$workingPath/$SCHOLIX_PATH_NAME"

  /** Utility to parse Date in ISO8601 to epochMillis
    * @param inputDate The String represents an input date in ISO8601
    * @return The relative epochMillis of parsed date
    */
  def ISO8601toEpochMillis(inputDate: String): Long = {
    simpleFormatter.parse(inputDate).getTime
  }

  /** This method tries to retrieve the last collection date from all datacite
    * records in HDFS.
    * This method should be called before indexing scholexplorer to retrieve
    * the delta of Datacite record to download, since from the generation of
    * raw graph to the generation of Scholexplorer sometimes it takes 20 days
    * @param spark
    * @param entitiesPath
    * @return the last collection date from the current scholexplorer Graph of the datacite records
    */
  def retrieveLastCollectedFrom(spark: SparkSession, entitiesPath: String): Long = {
    log.info("Retrieve last entities collected From")

    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]
    implicit val resultEncoder: Encoder[Result] = Encoders.kryo[Result]
    import spark.implicits._

    val entitiesDS = spark.read
      .load(s"$entitiesPath/*")
      .as[Oaf]
      .filter(o => o.isInstanceOf[Result])
      .map(r => r.asInstanceOf[Result])

    val date = entitiesDS
      .filter(r => r.getDateofcollection != null)
      .map(_.getDateofcollection)
      .select(max("value"))
      .first
      .getString(0)

    ISO8601toEpochMillis(date) / 1000
  }

  /** The method of update Datacite relationships on Scholexplorer
    * needs some utilities data structures
    * One is the scholixResource DS that stores all the nodes in the Scholix Graph
    * in format ScholixResource
    * @param summaryPath the path of the summary in Scholix
    * @param workingPath the working path
    * @param spark the spark session
    */
  def generateScholixResource(
    summaryPath: String,
    workingPath: String,
    spark: SparkSession
  ): Unit = {
    implicit val summaryEncoder: Encoder[ScholixSummary] = Encoders.kryo[ScholixSummary]
    implicit val scholixResourceEncoder: Encoder[ScholixResource] = Encoders.kryo[ScholixResource]

    log.info("Convert All summary to ScholixResource")
    spark.read
      .load(summaryPath)
      .as[ScholixSummary]
      .map(ScholixUtils.generateScholixResourceFromSummary)(scholixResourceEncoder)
      .filter(r => r.getIdentifier != null && r.getIdentifier.size > 0)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"${scholixResourcePath(workingPath)}_native")
  }

  /** This method convert the new Datacite Resource into Scholix Resource
    * Needed to fill the source and the type of Scholix Relationships
    * @param workingPath the Working Path
    * @param spark The spark Session
    */
  def addMissingScholixResource(workingPath: String, spark: SparkSession): Unit = {
    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]
    implicit val scholixResourceEncoder: Encoder[ScholixResource] = Encoders.kryo[ScholixResource]
    implicit val resultEncoder: Encoder[Result] = Encoders.kryo[Result]
    import spark.implicits._

    spark.read
      .load(dataciteOAFPath(workingPath))
      .as[Oaf]
      .filter(_.isInstanceOf[Result])
      .map(_.asInstanceOf[Result])
      .map(ScholixUtils.generateScholixResourceFromResult)
      .filter(r => r.getIdentifier != null && r.getIdentifier.size > 0)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"${scholixResourcePath(workingPath)}_update")

    val update = spark.read.load(s"${scholixResourcePath(workingPath)}_update").as[ScholixResource]
    val native = spark.read.load(s"${scholixResourcePath(workingPath)}_native").as[ScholixResource]
    val graph = update
      .union(native)
      .groupByKey(_.getDnetIdentifier)
      .reduceGroups((a, b) => if (a != null && a.getDnetIdentifier != null) a else b)
      .map(_._2)
    graph.write.mode(SaveMode.Overwrite).save(s"${scholixResourcePath(workingPath)}_graph")
  }

  /** This method get and Transform only datacite records with
    * timestamp greater than timestamp
    * @param datacitePath the datacite input Path
    * @param timestamp the timestamp
    * @param workingPath the working path where save the generated Dataset
    * @param spark SparkSession
    * @param vocabularies Vocabularies needed for transformation
    */

  def getDataciteUpdate(
    datacitePath: String,
    timestamp: Long,
    workingPath: String,
    spark: SparkSession,
    vocabularies: VocabularyGroup
  ): Long = {
    import spark.implicits._
    val ds = spark.read.load(datacitePath).as[DataciteType]
    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]
    val total = ds.filter(_.timestamp >= timestamp).count()
    if (total > 0) {
      ds.filter(_.timestamp >= timestamp)
        .flatMap(d =>
          DataciteToOAFTransformation
            .generateOAF(d.json, d.timestamp, d.timestamp, vocabularies, exportLinks = true)
        )
        .flatMap(i => fixRelations(i))
        .filter(i => i != null)
        .write
        .mode(SaveMode.Overwrite)
        .save(dataciteOAFPath(workingPath))
    }
    total
  }

  /** After added the new ScholixResource, we need to update the scholix Pid Map
    * to intersected with the new Datacite Relations
    *
    * @param workingPath The working Path starting from save the new Map
    * @param spark the spark session
    */
  def generatePidMap(workingPath: String, spark: SparkSession): Unit = {
    implicit val scholixResourceEncoder: Encoder[ScholixResource] = Encoders.kryo[ScholixResource]
    import spark.implicits._
    spark.read
      .load(s"${scholixResourcePath(workingPath)}_graph")
      .as[ScholixResource]
      .flatMap(r =>
        r.getIdentifier.asScala
          .map(i => DHPUtils.generateUnresolvedIdentifier(i.getIdentifier, i.getSchema))
          .map(t => (t, r.getDnetIdentifier))
      )(Encoders.tuple(Encoders.STRING, Encoders.STRING))
      .groupByKey(_._1)
      .reduceGroups((a, b) => if (a != null && a._2 != null) a else b)
      .map(_._2)(Encoders.tuple(Encoders.STRING, Encoders.STRING))
      .write
      .mode(SaveMode.Overwrite)
      .save(pidMapPath(workingPath))
  }

  /** This method resolve the datacite relation and filter the resolved
    * relation
    * @param workingPath the working path
    * @param spark the spark session
    */

  def resolveUpdateRelation(workingPath: String, spark: SparkSession): Unit = {
    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]
    implicit val relationEncoder: Encoder[Relation] = Encoders.kryo[Relation]
    import spark.implicits._

    val pidMap = spark.read.load(pidMapPath(workingPath)).as[(String, String)]

    val unresolvedRelations: Dataset[(String, Relation)] = spark.read
      .load(dataciteOAFPath(workingPath))
      .as[Oaf]
      .filter(_.isInstanceOf[Relation])
      .map(_.asInstanceOf[Relation])
      .map { r =>
        if (r.getSource.startsWith("unresolved"))
          (r.getSource, r)
        else
          (r.getTarget, r)
      }(Encoders.tuple(Encoders.STRING, relationEncoder))

    unresolvedRelations
      .joinWith(pidMap, unresolvedRelations("_1").equalTo(pidMap("_1")))
      .map(t => {
        val r = t._1._2
        val resolvedIdentifier = t._2._2
        if (r.getSource.startsWith("unresolved"))
          r.setSource(resolvedIdentifier)
        else
          r.setTarget(resolvedIdentifier)
        r
      })(relationEncoder)
      .filter(r => !(r.getSource.startsWith("unresolved") || r.getTarget.startsWith("unresolved")))
      .write
      .mode(SaveMode.Overwrite)
      .save(resolvedRelationPath(workingPath))
  }

  /** This method generate scholix starting from resolved relation
    *
    * @param workingPath
    * @param spark
    */
  def generateScholixUpdate(workingPath: String, spark: SparkSession): Unit = {
    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]
    implicit val scholixEncoder: Encoder[Scholix] = Encoders.kryo[Scholix]
    implicit val scholixResourceEncoder: Encoder[ScholixResource] = Encoders.kryo[ScholixResource]
    implicit val relationEncoder: Encoder[Relation] = Encoders.kryo[Relation]
    implicit val intermediateEncoder: Encoder[(String, Scholix)] =
      Encoders.tuple(Encoders.STRING, scholixEncoder)

    val relations: Dataset[(String, Relation)] = spark.read
      .load(resolvedRelationPath(workingPath))
      .as[Relation]
      .map(r => (r.getSource, r))(Encoders.tuple(Encoders.STRING, relationEncoder))

    val id_summary: Dataset[(String, ScholixResource)] = spark.read
      .load(s"${scholixResourcePath(workingPath)}_graph")
      .as[ScholixResource]
      .map(r => (r.getDnetIdentifier, r))(Encoders.tuple(Encoders.STRING, scholixResourceEncoder))

    id_summary.cache()

    relations
      .joinWith(id_summary, relations("_1").equalTo(id_summary("_1")), "inner")
      .map(t => (t._1._2.getTarget, ScholixUtils.scholixFromSource(t._1._2, t._2._2)))
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingPath/scholix_one_verse")

    val source_scholix: Dataset[(String, Scholix)] =
      spark.read.load(s"$workingPath/scholix_one_verse").as[(String, Scholix)]

    source_scholix
      .joinWith(id_summary, source_scholix("_1").equalTo(id_summary("_1")), "inner")
      .map(t => {
        val target: ScholixResource = t._2._2
        val scholix: Scholix = t._1._2
        ScholixUtils.generateCompleteScholix(scholix, target)
      })(scholixEncoder)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingPath/scholix")
  }

  /** Here all the spark applications runs this method
    * where the whole logic of the spark node is defined
    */
  override def run(): Unit = {
    val sourcePath = parser.get("sourcePath")
    log.info(s"SourcePath is '$sourcePath'")

    val datacitePath = parser.get("datacitePath")
    log.info(s"DatacitePath is '$datacitePath'")

    val workingPath = parser.get("workingSupportPath")
    log.info(s"workingPath is '$workingPath'")

    val isLookupUrl: String = parser.get("isLookupUrl")
    log.info("isLookupUrl: {}", isLookupUrl)

    val isLookupService = ISLookupClientFactory.getLookUpService(isLookupUrl)
    val vocabularies = VocabularyGroup.loadVocsFromIS(isLookupService)
    require(vocabularies != null)

    val updateDS: Boolean = "true".equalsIgnoreCase(parser.get("updateDS"))
    log.info(s"updateDS is '$updateDS'")

    var lastCollectionDate = 0L
    if (updateDS) {
      generateScholixResource(s"$sourcePath/provision/summaries", workingPath, spark)
      log.info("Retrieve last entities collected From starting from scholix Graph")
      lastCollectionDate = retrieveLastCollectedFrom(spark, s"$sourcePath/entities")
    } else {
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      fs.delete(new Path(s"${scholixResourcePath(workingPath)}_native"), true)
      fs.rename(
        new Path(s"${scholixResourcePath(workingPath)}_graph"),
        new Path(s"${scholixResourcePath(workingPath)}_native")
      )
      lastCollectionDate = retrieveLastCollectedFrom(spark, dataciteOAFPath(workingPath))
    }

    val numRecords =
      getDataciteUpdate(datacitePath, lastCollectionDate, workingPath, spark, vocabularies)
    if (numRecords > 0) {
      addMissingScholixResource(workingPath, spark)
      generatePidMap(workingPath, spark)
      resolveUpdateRelation(workingPath, spark)
      generateScholixUpdate(workingPath, spark)
    }
  }
}

object SparkRetrieveDataciteDelta {
  val log: Logger = LoggerFactory.getLogger(SparkRetrieveDataciteDelta.getClass)

  def main(args: Array[String]): Unit = {
    new SparkRetrieveDataciteDelta(
      "/eu/dnetlib/dhp/sx/graph/retrieve_datacite_delta_params.json",
      args,
      log
    ).initialize().run()
  }
}
