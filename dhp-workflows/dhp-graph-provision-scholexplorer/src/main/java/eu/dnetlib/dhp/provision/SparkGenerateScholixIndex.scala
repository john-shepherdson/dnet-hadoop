package eu.dnetlib.dhp.provision

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.provision.scholix.{Scholix, ScholixResource}
import eu.dnetlib.dhp.provision.scholix.summary.ScholixSummary
import eu.dnetlib.dhp.schema.oaf.Relation
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode, SparkSession}

object SparkGenerateScholixIndex {



  def getScholixAggregator(): Aggregator[(String, Scholix), Scholix, Scholix] = new Aggregator[(String, Scholix), Scholix, Scholix]{

    override def zero: Scholix = new Scholix()

    override def reduce(b: Scholix, a: (String, Scholix)): Scholix = {
      b.mergeFrom(a._2)
      b
    }

    override def merge(wx: Scholix, wy: Scholix): Scholix = {
      wx.mergeFrom(wy)
      wx
    }
    override def finish(reduction: Scholix): Scholix = reduction

    override def bufferEncoder: Encoder[Scholix] =
      Encoders.kryo(classOf[Scholix])

    override def outputEncoder: Encoder[Scholix] =
      Encoders.kryo(classOf[Scholix])
  }


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

      }).repartition(6000).write.mode(SaveMode.Overwrite).save(s"$workingDirPath/scholix_source")

    val sTarget:Dataset[(String,Scholix)] = spark.read.load(s"$workingDirPath/scholix_source").as[(String, Scholix)]

    sTarget.joinWith(scholixSummary, sTarget("_1").equalTo(scholixSummary("_1")), "inner").map(i => {
      val summary = i._2._2
      val scholix = i._1._2

      val scholixResource = ScholixResource.fromSummary(summary)
      scholix.setTarget(scholixResource)
      scholix.generateIdentifier()
      scholix.generatelinkPublisher()
      scholix
    }).repartition(6000).write.mode(SaveMode.Overwrite).save(s"$workingDirPath/scholix_r")


    val finalScholix:Dataset[Scholix] = spark.read.load(s"$workingDirPath/scholix_r").as[Scholix]

    finalScholix.map(d => (d.getIdentifier, d))(Encoders.tuple(Encoders.STRING, scholixEncoder))
      .groupByKey(_._1)(Encoders.STRING)
      .agg(getScholixAggregator().toColumn)
      .map(p => p._2)
      .write.mode(SaveMode.Overwrite).save(s"$workingDirPath/scholix")

  }

}
