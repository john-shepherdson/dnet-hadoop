package eu.dnetlib.dhp.provision

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.provision.scholix.summary.ScholixSummary
import eu.dnetlib.dhp.schema.oaf.{Oaf, OafEntity, Relation}
import eu.dnetlib.dhp.schema.scholexplorer.{DLIDataset, DLIPublication, DLIUnknown}
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}

object SparkGenerateSummaryIndex {

  def main(args: Array[String]): Unit = {
    val parser = new ArgumentApplicationParser(IOUtils.toString(SparkGenerateSummaryIndex.getClass.getResourceAsStream("/eu/dnetlib/dhp/provision/input_generate_summary_parameters.json")))
    parser.parseArgument(args)
    val spark = SparkSession.builder.appName(SparkGenerateSummaryIndex.getClass.getSimpleName).master(parser.get("master")).getOrCreate

    val graphPath = parser.get("graphPath")
    val workingDirPath = parser.get("workingDirPath")

    implicit val relatedItemInfoEncoders: Encoder[RelatedItemInfo] = Encoders.bean(classOf[RelatedItemInfo])
    implicit  val datasetEncoder:Encoder[DLIDataset] = Encoders.kryo[DLIDataset]
    implicit  val publicationEncoder:Encoder[DLIPublication] = Encoders.kryo[DLIPublication]
    implicit  val relationEncoder:Encoder[Relation] = Encoders.kryo[Relation]
    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]
    implicit val oafWithIdEncoder: Encoder[(String, Oaf)] = Encoders.tuple(Encoders.STRING, oafEncoder)
    implicit val scholixSummaryEncoder: Encoder[ScholixSummary] = Encoders.kryo[ScholixSummary]
    implicit val scholixSummaryEncoderTuple: Encoder[(String,ScholixSummary)] = Encoders.tuple(Encoders.STRING,scholixSummaryEncoder)


    val pubs = spark.read.load(s"$graphPath/publication").as[Oaf].map(o => (o.asInstanceOf[DLIPublication].getId, o))
    val dats = spark.read.load(s"$graphPath/dataset").as[Oaf].map(o => (o.asInstanceOf[DLIDataset].getId, o))
    val ukn = spark.read.load(s"$graphPath/unknown").as[Oaf].map(o => (o.asInstanceOf[DLIUnknown].getId, o))


    val summary:Dataset[(String,ScholixSummary)] = pubs.union(dats).union(ukn).map(o =>{
      val s = ScholixSummary.fromOAF(o._2)
      (s.getId,s)
    })


    val relatedItemInfoDs:Dataset[RelatedItemInfo] = spark.read.load(s"$workingDirPath/relatedItemCount").as[RelatedItemInfo]


    summary.joinWith(relatedItemInfoDs, summary("_1").equalTo(relatedItemInfoDs("source")), "inner")
      .map(i => {
        val summary = i._1._2
        val relatedItemInfo = i._2
        summary.setRelatedDatasets(relatedItemInfo.getRelatedDataset)
        summary.setRelatedPublications(relatedItemInfo.getRelatedPublication)
        summary.setRelatedUnknown(relatedItemInfo.getRelatedUnknown)
        summary
      }).filter(s => s.getLocalIdentifier != null).write.mode(SaveMode.Overwrite).save(s"$workingDirPath/summary")















  }

}
