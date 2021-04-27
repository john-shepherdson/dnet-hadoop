package eu.dnetlib.sx.pangaea


import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}
import org.json4s
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import java.text.SimpleDateFormat
import java.util.Date


case class PangaeaDataModel(datestamp:String, identifier:String, xml:String) {}



object PangaeaUtils {


  def toDataset(input:String):PangaeaDataModel = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: json4s.JValue = parse(input)

    val  d = new Date()
    val s:String =  s"${new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")format(d)}Z"

    val ds = (json \ "internal-datestamp").extractOrElse[String](s)
    val identifier= (json \ "metadatalink").extractOrElse[String]("")
    val xml= (json \ "xml").extract[String]
    PangaeaDataModel(ds, identifier,xml)
  }


  def getDatasetAggregator(): Aggregator[(String, PangaeaDataModel), PangaeaDataModel, PangaeaDataModel] =   new Aggregator[(String, PangaeaDataModel), PangaeaDataModel, PangaeaDataModel]{


      override def zero: PangaeaDataModel = null

      override def reduce(b: PangaeaDataModel, a: (String, PangaeaDataModel)): PangaeaDataModel = {
        if (b == null)
          a._2
        else {
          if (a == null)
            b
          else {
            val ts1 = b.datestamp
            val ts2 = a._2.datestamp
            if (ts1 > ts2)
              b
            else
              a._2

          }
        }
      }

      override def merge(b1: PangaeaDataModel, b2: PangaeaDataModel): PangaeaDataModel = {
        if (b1 == null)
          b2
        else {
          if (b2 == null)
            b1
          else {
            val ts1 = b1.datestamp
            val ts2 = b2.datestamp
            if (ts1 > ts2)
              b1
            else
              b2

          }
        }
      }
      override def finish(reduction: PangaeaDataModel): PangaeaDataModel = reduction

      override def bufferEncoder: Encoder[PangaeaDataModel] = Encoders.kryo[PangaeaDataModel]

      override def outputEncoder: Encoder[PangaeaDataModel] = Encoders.kryo[PangaeaDataModel]
    }




}