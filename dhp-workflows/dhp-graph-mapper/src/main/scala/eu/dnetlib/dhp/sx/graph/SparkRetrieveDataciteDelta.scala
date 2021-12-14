package eu.dnetlib.dhp.sx.graph

import eu.dnetlib.dhp.application.AbstractScalaApplication
import eu.dnetlib.dhp.schema.oaf.Result
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.slf4j.Logger

class SparkRetrieveDataciteDelta (propertyPath:String, args:Array[String], log:Logger) extends  AbstractScalaApplication(propertyPath, args, log:Logger) {


  def retrieveLastCollectedFrom(spark:SparkSession, entitiesPath:String):String = {
    log.info("Retrieve last entities collected From")

    implicit val oafEncoder:Encoder[Result] = Encoders.kryo[Result]
    import spark.implicits._

    val entitiesDS = spark.read.load(s"$entitiesPath/*").as[Result]

    entitiesDS.filter(r => r.getDateofcollection!= null).map(_.getDateofcollection).select(max("value")).first.getString(0)



  }


  /**
   * Here all the spark applications runs this method
   * where the whole logic of the spark node is defined
   */
  override def run(): Unit = {
    val sourcePath = parser.get("sourcePath")
    log.info(s"SourcePath is '$sourcePath'")

    val datacitePath = parser.get("datacitePath")
    log.info(s"DatacitePath is '$datacitePath'")


    log.info("Retrieve last entities collected From")

    implicit val oafEncoder:Encoder[Result] = Encoders.kryo[Result]

    val lastCollectionDate = retrieveLastCollectedFrom(spark, s"$sourcePath/entities")









  }
}
