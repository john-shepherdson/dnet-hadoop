package eu.dnetlib.dhp.provision

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.provision.scholix.{Scholix, ScholixResource}
import eu.dnetlib.dhp.provision.scholix.summary.ScholixSummary
import eu.dnetlib.dhp.schema.oaf.Relation
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}

object SparkGenerateScholixIndex {


  def main(args: Array[String]): Unit = {
    val parser = new ArgumentApplicationParser(IOUtils.toString(SparkGenerateScholixIndex.getClass.getResourceAsStream("/eu/dnetlib/dhp/provision/input_generate_summary_parameters.json")))
    parser.parseArgument(args)
    val conf = new SparkConf
    conf.set("spark.sql.shuffle.partitions", "4000")
    val spark = SparkSession.builder.config(conf).appName(SparkGenerateScholixIndex.getClass.getSimpleName).master(parser.get("master")).getOrCreate

    val graphPath = parser.get("graphPath")
    val  workingDirPath = parser.get("workingDirPath")


    implicit  val summaryEncoder:Encoder[ScholixSummary] = Encoders.kryo[ScholixSummary]
    implicit  val relEncoder:Encoder[Relation] = Encoders.kryo[Relation]
    implicit  val scholixEncoder:Encoder[Scholix] = Encoders.kryo[Scholix]
    implicit val tupleScholix:Encoder[(String,Scholix)]=Encoders.tuple(Encoders.STRING, scholixEncoder)


    val scholixSummary:Dataset[(String,ScholixSummary)] = spark.read.load(s"$workingDirPath/summary").as[ScholixSummary]
      .map(s => (s.getId, s))(Encoders.tuple(Encoders.STRING, summaryEncoder))
    val sourceRelations:Dataset[(String,Relation)]= spark.read.load(s"$graphPath/relation").as[Relation]
      .map(r => (r.getSource,r))(Encoders.tuple(Encoders.STRING, relEncoder))

    scholixSummary.joinWith(sourceRelations, scholixSummary("_1").equalTo(sourceRelations("_1")), "inner")
      .map(r=> {
        val summary = r._1._2
        val relation = r._2._2

        (relation.getTarget, Scholix.generateScholixWithSource(summary,relation))

      }).write.mode(SaveMode.Overwrite).save(s"$workingDirPath/scholix_source")

    val sTarget:Dataset[(String,Scholix)] = spark.read.load(s"$workingDirPath/scholix_source").as[(String, Scholix)]

    sTarget.joinWith(scholixSummary, sTarget("_1").equalTo(scholixSummary("_1")), "inner").map(i => {
      val summary = i._2._2
      val scholix = i._1._2

      val scholixResource = ScholixResource.fromSummary(summary)
      scholix.setTarget(scholixResource)
      scholix.generateIdentifier()
      scholix.generatelinkPublisher()
      scholix
    }).write.mode(SaveMode.Overwrite).save(s"$workingDirPath/scholix")



  }

}
