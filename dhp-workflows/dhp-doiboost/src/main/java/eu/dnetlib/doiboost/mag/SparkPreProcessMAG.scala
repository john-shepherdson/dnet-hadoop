package eu.dnetlib.doiboost.mag

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.{Journal, Publication, StructuredProperty}
import eu.dnetlib.doiboost.DoiBoostMappingUtil
import eu.dnetlib.doiboost.DoiBoostMappingUtil.{asField, createSP}
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

object SparkPreProcessMAG {
  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(getClass.getResourceAsStream("/eu/dnetlib/dhp/doiboost/mag/preprocess_mag_params.json")))
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()

    val sourcePath = parser.get("sourcePath")
    import spark.implicits._
    implicit val mapEncoderPubs: Encoder[Publication] = org.apache.spark.sql.Encoders.kryo[Publication]
    implicit val tupleForJoinEncoder: Encoder[(String, Publication)] = Encoders.tuple(Encoders.STRING, mapEncoderPubs)

    logger.info("Phase 1) make uninque DOI in Papers:")
    val d: Dataset[MagPapers] = spark.read.load(s"${parser.get("sourcePath")}/Papers").as[MagPapers]

    // Filtering Papers with DOI, and since for the same DOI we have multiple version of item with different PapersId we get the last one
    val result: RDD[MagPapers] = d.where(col("Doi").isNotNull).rdd.map { p: MagPapers => Tuple2(p.Doi, p) }.reduceByKey { case (p1: MagPapers, p2: MagPapers) =>
      var r = if (p1 == null) p2 else p1
      if (p1 != null && p2 != null) {
        if (p1.CreatedDate != null && p2.CreatedDate != null) {
          if (p1.CreatedDate.before(p2.CreatedDate))
            r = p1
          else
            r = p2
        } else {
          r = if (p1.CreatedDate == null) p2 else p1
        }
      }
      r
    }.map(_._2)


    val distinctPaper: Dataset[MagPapers] = spark.createDataset(result)
    distinctPaper.write.mode(SaveMode.Overwrite).save(s"${parser.get("targetPath")}/Papers_distinct")
    logger.info(s"Total number of element: ${result.count()}")

    logger.info("Phase 3) Group Author by PaperId")
    val authors = spark.read.load(s"$sourcePath/Authors").as[MagAuthor]

    val affiliation = spark.read.load(s"$sourcePath/Affiliations").as[MagAffiliation]
    val paperAuthorAffiliation = spark.read.load(s"$sourcePath/PaperAuthorAffiliations").as[MagPaperAuthorAffiliation]

    paperAuthorAffiliation.joinWith(authors, paperAuthorAffiliation("AuthorId").equalTo(authors("AuthorId")))
      .map { case (a: MagPaperAuthorAffiliation, b: MagAuthor) => (a.AffiliationId, MagPaperAuthorDenormalized(a.PaperId, b, null)) }
      .joinWith(affiliation, affiliation("AffiliationId").equalTo(col("_1")), "left")
      .map(s => {
        val mpa = s._1._2
        val af = s._2
        if (af != null) {
          MagPaperAuthorDenormalized(mpa.PaperId, mpa.author, af.DisplayName)
        } else
          mpa
      }).groupBy("PaperId").agg(collect_list(struct($"author", $"affiliation")).as("authors"))
      .write.mode(SaveMode.Overwrite).save(s"${parser.get("targetPath")}/merge_step_1_paper_authors")

    logger.info("Phase 4) create First Version of publication Entity with Paper Journal and Authors")

    val journals = spark.read.load(s"$sourcePath/Journals").as[MagJournal]

    val papers = spark.read.load((s"${parser.get("targetPath")}/Papers_distinct")).as[MagPapers]

    val paperWithAuthors = spark.read.load(s"${parser.get("targetPath")}/merge_step_1_paper_authors").as[MagPaperWithAuthorList]

    val firstJoin = papers.joinWith(journals, papers("JournalId").equalTo(journals("JournalId")), "left")
    firstJoin.joinWith(paperWithAuthors, firstJoin("_1.PaperId").equalTo(paperWithAuthors("PaperId")), "left")
      .map { a: ((MagPapers, MagJournal), MagPaperWithAuthorList) => ConversionUtil.createOAFFromJournalAuthorPaper(a) }.write.mode(SaveMode.Overwrite).save(s"${parser.get("targetPath")}/merge_step_2")


    var magPubs: Dataset[(String, Publication)] = spark.read.load(s"${parser.get("targetPath")}/merge_step_2").as[Publication].map(p => (ConversionUtil.extractMagIdentifier(p.getOriginalId.asScala), p)).as[(String, Publication)]


    val conference = spark.read.load(s"$sourcePath/ConferenceInstances").select($"ConferenceInstanceId".as("ci"), $"DisplayName", $"Location", $"StartDate",$"EndDate" )
    val conferenceInstance = conference.joinWith(papers, papers("ConferenceInstanceId").equalTo(conference("ci"))).select($"_1.ci", $"_1.DisplayName", $"_1.Location", $"_1.StartDate",$"_1.EndDate", $"_2.PaperId").as[MagConferenceInstance]


    magPubs.joinWith(conferenceInstance, col("_1").equalTo(conferenceInstance("PaperId")), "left")
      .map(p => {
        val publication:Publication= p._1._2
        val ci:MagConferenceInstance = p._2

        if (ci!= null){

          val j:Journal = new Journal
          if (ci.Location.isDefined)
            j.setConferenceplace(ci.Location.get)
          j.setName(ci.DisplayName.get)
          if (ci.StartDate.isDefined && ci.EndDate.isDefined)
            {
              j.setConferencedate(s"${ci.StartDate.get.toString} - ${ci.EndDate.get.toString}")
            }

          publication.setJournal(j)
        }
        publication

      }).write.mode(SaveMode.Overwrite).save(s"${parser.get("targetPath")}/merge_step_2_conference")


    magPubs= spark.read.load(s"${parser.get("targetPath")}/merge_step_2_conference").as[Publication].map(p => (ConversionUtil.extractMagIdentifier(p.getOriginalId.asScala), p)).as[(String, Publication)]

    val paperUrlDataset = spark.read.load(s"$sourcePath/PaperUrls").as[MagPaperUrl].groupBy("PaperId").agg(collect_list(struct("sourceUrl")).as("instances")).as[MagUrl]


    logger.info("Phase 5) enrich publication with URL and Instances")

    magPubs.joinWith(paperUrlDataset, col("_1").equalTo(paperUrlDataset("PaperId")), "left")
      .map { a: ((String, Publication), MagUrl) => ConversionUtil.addInstances((a._1._2, a._2)) }
      .write.mode(SaveMode.Overwrite)
      .save(s"${parser.get("targetPath")}/merge_step_3")


    logger.info("Phase 6) Enrich Publication with description")
    val pa = spark.read.load(s"${parser.get("sourcePath")}/PaperAbstractsInvertedIndex").as[MagPaperAbstract]
    pa.map(ConversionUtil.transformPaperAbstract).write.mode(SaveMode.Overwrite).save(s"${parser.get("targetPath")}/PaperAbstract")

    val paperAbstract = spark.read.load((s"${parser.get("targetPath")}/PaperAbstract")).as[MagPaperAbstract]


    magPubs = spark.read.load(s"${parser.get("targetPath")}/merge_step_3").as[Publication].map(p => (ConversionUtil.extractMagIdentifier(p.getOriginalId.asScala), p)).as[(String, Publication)]

    magPubs.joinWith(paperAbstract, col("_1").equalTo(paperAbstract("PaperId")), "left").map(p => {
      val pub = p._1._2
      val abst = p._2
      if (abst != null) {
        pub.setDescription(List(asField(abst.IndexedAbstract)).asJava)
      }
      pub
    }
    ).write.mode(SaveMode.Overwrite).save(s"${parser.get("targetPath")}/merge_step_4")


    logger.info("Phase 7) Enrich Publication with FieldOfStudy")

    magPubs = spark.read.load(s"${parser.get("targetPath")}/merge_step_4").as[Publication].map(p => (ConversionUtil.extractMagIdentifier(p.getOriginalId.asScala), p)).as[(String, Publication)]

    val fos = spark.read.load(s"$sourcePath/FieldsOfStudy").select($"FieldOfStudyId".alias("fos"), $"DisplayName", $"MainType")

    val pfos = spark.read.load(s"$sourcePath/PaperFieldsOfStudy")

    val paperField = pfos.joinWith(fos, fos("fos").equalTo(pfos("FieldOfStudyId")))
      .select($"_1.FieldOfStudyId", $"_2.DisplayName", $"_2.MainType", $"_1.PaperId", $"_1.Score")
      .groupBy($"PaperId").agg(collect_list(struct($"FieldOfStudyId", $"DisplayName", $"MainType", $"Score")).as("subjects"))
      .as[MagFieldOfStudy]

    magPubs.joinWith(paperField, col("_1").equalTo(paperField("PaperId")), "left").
      map(item => {
        val publication = item._1._2
        val fieldOfStudy = item._2
        if (fieldOfStudy != null && fieldOfStudy.subjects != null && fieldOfStudy.subjects.nonEmpty) {
          val p: List[StructuredProperty] = fieldOfStudy.subjects.flatMap(s => {
            val s1 = createSP(s.DisplayName, "keywords", "dnet:subject_classification_typologies")
            val di = DoiBoostMappingUtil.generateDataInfo(s.Score.toString)
            var resList: List[StructuredProperty] = List(s1)
            if (s.MainType.isDefined) {
              val maintp = s.MainType.get
              val s2 = createSP(s.MainType.get, "keywords", "dnet:subject_classification_typologies")
              s2.setDataInfo(di)
              resList = resList ::: List(s2)
              if (maintp.contains(".")) {
                val s3 = createSP(maintp.split("\\.").head, "keywords", "dnet:subject_classification_typologies")
                s3.setDataInfo(di)
                resList = resList ::: List(s3)
              }
            }
            resList
          })
          publication.setSubject(p.asJava)
        }
        publication
      }).map { s: Publication => s }(Encoders.bean(classOf[Publication])).write.mode(SaveMode.Overwrite).save(s"${parser.get("targetPath")}/mag_publication")
  }
}
