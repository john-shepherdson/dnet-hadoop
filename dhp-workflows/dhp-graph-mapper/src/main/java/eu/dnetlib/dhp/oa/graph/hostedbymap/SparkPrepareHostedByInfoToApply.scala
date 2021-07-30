package eu.dnetlib.dhp.oa.graph.hostedbymap

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.oa.graph.hostedbymap.model.{DatasourceInfo, EntityInfo}
import eu.dnetlib.dhp.schema.oaf.{Datasource, Journal, Publication}
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.{Logger, LoggerFactory}

object SparkPrepareHostedByInfoToApply {


  implicit val mapEncoderDSInfo: Encoder[DatasourceInfo] = Encoders.kryo[DatasourceInfo]
  implicit val mapEncoderPInfo: Encoder[EntityInfo] = Encoders.kryo[EntityInfo]

  def getList(id: String, j: Journal, name: String ) : List[EntityInfo] = {
    var lst:List[EntityInfo] = List()


    if (j.getIssnLinking != null && !j.getIssnLinking.equals("")){
      lst = EntityInfo.newInstance(id, j.getIssnLinking, name) :: lst
    }
    if (j.getIssnOnline != null && !j.getIssnOnline.equals("")){
      lst = EntityInfo.newInstance(id, j.getIssnOnline, name) :: lst
    }
    if (j.getIssnPrinted != null && !j.getIssnPrinted.equals("")){
      lst = EntityInfo.newInstance(id, j.getIssnPrinted, name) :: lst
    }
    lst
  }

  def prepareResultInfo(spark:SparkSession, publicationPath:String) : Dataset[EntityInfo] = {
    implicit val mapEncoderPubs: Encoder[Publication] = Encoders.bean(classOf[Publication])

    val mapper = new ObjectMapper()

    val dd : Dataset[Publication] = spark.read.textFile(publicationPath)
      .map(r => mapper.readValue(r, classOf[Publication]))

    dd.filter(p => p.getJournal != null ).flatMap(p => getList(p.getId, p.getJournal, ""))

  }



  def prepareDatasourceInfo(spark:SparkSession, datasourcePath:String) : Dataset[DatasourceInfo] = {
    implicit val mapEncoderDats: Encoder[Datasource] = Encoders.bean(classOf[Datasource])

    val mapper = new ObjectMapper()

    val dd : Dataset[Datasource] = spark.read.textFile(datasourcePath)
      .map(r => mapper.readValue(r, classOf[Datasource]))

    dd.filter(d => d.getJournal != null ).map(d => DatasourceInfo.newInstance(d.getId, d.getOfficialname.getValue,
      d.getJournal.getIssnPrinted, d.getJournal.getIssnOnline, d.getJournal.getIssnLinking))

  }
  def toHostedByItem(input:String): HostedByItemType = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats

    lazy val json: json4s.JValue = parse(input)
    val c :Map[String,HostedByItemType] = json.extract[Map[String, HostedByItemType]]
    c.values.head
  }

  def explodeJournalInfo(input: DatasourceInfo): List[EntityInfo] = {
    var lst : List[EntityInfo] = List()
    if (input.getEissn != null && !input.getEissn.equals("")){
      lst = EntityInfo.newInstance(input.getId, input.getEissn, input.getOfficialname, input.getOpenAccess) :: lst
    }

    lst
  }

  def main(args: Array[String]): Unit = {


    val logger: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(IOUtils.toString(getClass.getResourceAsStream("/eu/dnetlib/dhp/oa/graph/hostedbymap/hostedby_prepare_params.json")))
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master")).getOrCreate()


    val graphPath = parser.get("graphPath")

    val outputPath = parser.get("outputPath")
    val hostedByMapPath = parser.get("hostedByMapPath")


    implicit val formats = DefaultFormats


    logger.info("Getting the Datasources")

    import spark.implicits._

    //STEP1: leggere le DS e creare le entries {dsid, dsofficialname, issn, eissn, lissn, openaccess}
    val datasourceInfoDataset: Dataset[DatasourceInfo] = prepareDatasourceInfo(spark, "$graphPath/datasource")

    //STEP2: leggere la hostedbymap e raggruppare per datasource id
    val hostedByDataset  = Aggregators.hostedByToSingleDSId(spark.createDataset(spark.sparkContext.textFile(hostedByMapPath).map(toHostedByItem)))

    //STEP3: eseguire una join fra le datasource e la hostedby map (left) per settare se la datasource e' open access o no
    //ed esplodere l'info della datasource per ogni journal id diverso da nullo
    val join : Dataset[EntityInfo] = datasourceInfoDataset.joinWith(hostedByDataset,
      datasourceInfoDataset.col("id").equalTo(hostedByDataset.col("id"), "left"))
      .map(t2 => {
        val dsi : DatasourceInfo = t2._1
        if(t2._2 != null){
          dsi.setOpenAccess(t2._2.openAccess)
        }
        dsi
      }).flatMap(explodeJournalInfo)

    //STEP4: creare la mappa publication id issn, eissn, lissn esplosa
    val resultInfoDataset:Dataset[EntityInfo] = prepareResultInfo(spark, "$graphPath/publication")

    //STEP5: join di join con resultInfo sul journal_id dal result con left
    // e riduzione di tutti i result con lo stesso id in una unica entry
    Aggregators.resultToSingleId(resultInfoDataset.joinWith(join, resultInfoDataset.col("journal_id").equalTo(join.col("journal_id")), "left")
      .map(t2 => {
        val res: EntityInfo = t2._1
        if(t2._2 != null ){
          val ds = t2._2
          res.setHb_id(ds.getId)
          res.setOpenaccess(ds.getOpenaccess)
          res.setName(ds.getName)
        }
        res
      })).write.mode(SaveMode.Overwrite).option("compression", "gzip").json(outputPath)



  }

}
