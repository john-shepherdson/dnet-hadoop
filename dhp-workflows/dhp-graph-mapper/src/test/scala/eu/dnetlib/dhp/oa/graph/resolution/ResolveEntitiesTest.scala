package eu.dnetlib.dhp.oa.graph.resolution

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.schema.common.EntityType
import eu.dnetlib.dhp.schema.oaf.utils.OafMapperUtils
import eu.dnetlib.dhp.schema.oaf.{Publication, Result, StructuredProperty}
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.{AfterAll, BeforeAll, Disabled, Test, TestInstance}

import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._
import scala.io.Source

@TestInstance(Lifecycle.PER_CLASS)
class ResolveEntitiesTest extends Serializable {

  var workingDir: Path = null

  val FAKE_TITLE = "FAKETITLE"
  val FAKE_SUBJECT = "FAKESUBJECT"

  var sparkSession: Option[SparkSession] = None

  @BeforeAll
  def setUp(): Unit = {
    workingDir = Files.createTempDirectory(getClass.getSimpleName)

    val conf = new SparkConf()
    sparkSession = Some(
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master("local[*]")
        .getOrCreate()
    )
    populateDatasets(sparkSession.get)
    generateUpdates(sparkSession.get)

  }

  @AfterAll
  def tearDown(): Unit = {
    FileUtils.deleteDirectory(workingDir.toFile)
    sparkSession.get.stop()

  }

  def generateUpdates(spark: SparkSession): Unit = {
    val template = Source.fromInputStream(this.getClass.getResourceAsStream("updates")).mkString

    val pids: List[String] = template.linesWithSeparators
      .map(l => l.stripLineEnd)
      .map { id =>
        val r = new Result
        r.setId(id.toLowerCase.trim)
        r.setSubject(
          List(
            OafMapperUtils.subject(
              FAKE_SUBJECT,
              OafMapperUtils.qualifier("fos", "fosCS", "fossSchema", "fossiFIgo"),
              null
            )
          ).asJava
        )
        r.setTitle(
          List(
            OafMapperUtils.structuredProperty(
              FAKE_TITLE,
              OafMapperUtils.qualifier("fos", "fosCS", "fossSchema", "fossiFIgo"),
              null
            )
          ).asJava
        )
        r
      }
      .map { r =>
        val mapper = new ObjectMapper()

        mapper.writeValueAsString(r)
      }
      .toList

    val sc = spark.sparkContext

    println(sc.parallelize(pids).count())

    spark
      .createDataset(sc.parallelize(pids))(Encoders.STRING)
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .text(s"$workingDir/updates")

    import spark.implicits._
    implicit val resEncoder: Encoder[Result] = Encoders.bean(classOf[Result])
    val ds = spark.read
      .text(s"$workingDir/updates")
      .as[String]
      .map { s =>
        val mapper = new ObjectMapper()
        mapper.readValue(s, classOf[Result])
      }
      .collect()

    assertEquals(4, ds.length)
    ds.foreach { r => assertNotNull(r.getSubject) }
    ds.foreach { r => assertEquals(1, r.getSubject.size()) }
    ds.foreach { r => assertNotNull(r.getTitle) }
    ds.foreach { r => assertEquals(1, r.getTitle.size()) }

    ds.flatMap(r => r.getTitle.asScala.map(t => t.getValue))
      .foreach(t => assertEquals(FAKE_TITLE, t))
    ds.flatMap(r => r.getSubject.asScala.map(t => t.getValue))
      .foreach(t => assertEquals(FAKE_SUBJECT, t))

    println("generated Updates")
  }

  def populateDatasets(spark: SparkSession): Unit = {
    import spark.implicits._
    val entities = SparkResolveEntities.entities

    entities.foreach { e =>
      val template = Source.fromInputStream(this.getClass.getResourceAsStream(s"$e")).mkString
      spark
        .createDataset(spark.sparkContext.parallelize(template.linesWithSeparators.map(l => l.stripLineEnd).toList))
        .as[String]
        .write
        .option("compression", "gzip")
        .text(s"$workingDir/graph/$e")
      println(s"Created Dataset $e")
    }
    SparkResolveRelation.extractPidResolvedTableFromJsonRDD(
      spark,
      s"$workingDir/graph",
      s"$workingDir/work"
    )

  }

  @Test
  def testResolution(): Unit = {
    val spark: SparkSession = sparkSession.get
    implicit val resEncoder: Encoder[Result] = Encoders.kryo(classOf[Result])
    SparkResolveEntities.resolveEntities(spark, s"$workingDir/work", s"$workingDir/updates")

    val ds = spark.read.load(s"$workingDir/work/resolvedEntities").as[Result]

    assertEquals(3, ds.count())

    ds.collect().foreach { r =>
      assertTrue(r.getId.startsWith("50"))
    }
  }

  private def structuredPContainsValue(
    l: java.util.List[StructuredProperty],
    exptectedValue: String
  ): Boolean = {
    l.asScala.exists(p => p.getValue != null && p.getValue.equalsIgnoreCase(exptectedValue))
  }

  @Test
  def testUpdate(): Unit = {
    val spark: SparkSession = sparkSession.get
    import spark.implicits._
    implicit val resEncoder: Encoder[Result] = Encoders.kryo(classOf[Result])
    val m = new ObjectMapper()
    SparkResolveEntities.resolveEntities(spark, s"$workingDir/work", s"$workingDir/updates")
    SparkResolveEntities.generateResolvedEntities(
      spark,
      s"$workingDir/work",
      s"$workingDir/graph",
      s"$workingDir/target"
    )

    val pubDS: Dataset[Result] = spark.read
      .text(s"$workingDir/target/publication")
      .as[String]
      .map(s => SparkResolveEntities.deserializeObject(s, EntityType.publication))
    val t = pubDS
      .filter(p => p.getTitle != null && p.getSubject != null)
      .filter(p => p.getTitle.asScala.exists(t => t.getValue.equalsIgnoreCase("FAKETITLE")))
      .count()

    var ct = pubDS.count()
    var et = pubDS
      .filter(p => p.getTitle != null && p.getTitle.asScala.forall(t => t.getValue != null && t.getValue.nonEmpty))
      .count()

    assertEquals(ct, et)

    val datDS: Dataset[Result] = spark.read
      .text(s"$workingDir/target/dataset")
      .as[String]
      .map(s => SparkResolveEntities.deserializeObject(s, EntityType.dataset))
    val td = datDS
      .filter(p => p.getTitle != null && p.getSubject != null)
      .filter(p => p.getTitle.asScala.exists(t => t.getValue.equalsIgnoreCase("FAKETITLE")))
      .count()
    ct = datDS.count()
    et = datDS
      .filter(p => p.getTitle != null && p.getTitle.asScala.forall(t => t.getValue != null && t.getValue.nonEmpty))
      .count()
    assertEquals(ct, et)

    val softDS: Dataset[Result] = spark.read
      .text(s"$workingDir/target/software")
      .as[String]
      .map(s => SparkResolveEntities.deserializeObject(s, EntityType.software))
    val ts = softDS
      .filter(p => p.getTitle != null && p.getSubject != null)
      .filter(p => p.getTitle.asScala.exists(t => t.getValue.equalsIgnoreCase("FAKETITLE")))
      .count()
    ct = softDS.count()
    et = softDS
      .filter(p => p.getTitle != null && p.getTitle.asScala.forall(t => t.getValue != null && t.getValue.nonEmpty))
      .count()
    assertEquals(ct, et)

    val orpDS: Dataset[Result] = spark.read
      .text(s"$workingDir/target/otherresearchproduct")
      .as[String]
      .map(s => SparkResolveEntities.deserializeObject(s, EntityType.otherresearchproduct))
    val to = orpDS
      .filter(p => p.getTitle != null && p.getSubject != null)
      .filter(p => p.getTitle.asScala.exists(t => t.getValue.equalsIgnoreCase("FAKETITLE")))
      .count()

    ct = orpDS.count()
    et = orpDS
      .filter(p => p.getTitle != null && p.getTitle.asScala.forall(t => t.getValue != null && t.getValue.nonEmpty))
      .count()
    assertEquals(ct, et)

    assertEquals(0, t)
    assertEquals(2, td)
    assertEquals(1, ts)
    assertEquals(0, to)

  }

  @Test
  @Disabled
  def testMerge(): Unit = {

    val r = new Result
    r.setSubject(
      List(
        OafMapperUtils.subject(
          FAKE_SUBJECT,
          OafMapperUtils.qualifier("fos", "fosCS", "fossSchema", "fossiFIgo"),
          null
        )
      ).asJava
    )

    val mapper = new ObjectMapper()

    val p = mapper.readValue(
      Source
        .fromInputStream(this.getClass.getResourceAsStream(s"publication"))
        .mkString
        .linesWithSeparators
        .map(l => l.stripLineEnd)
        .next(),
      classOf[Publication]
    )

    // TODO should be reimplemented
    //r.mergeFrom(p)

    println(mapper.writeValueAsString(r))

  }

}
