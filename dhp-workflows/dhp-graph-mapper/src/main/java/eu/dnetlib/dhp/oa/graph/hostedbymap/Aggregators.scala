package eu.dnetlib.dhp.oa.graph.hostedbymap

import org.apache.spark.sql.{Dataset, Encoder, Encoders, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator


case class HostedByItemType(id: String, officialname: String, issn: String, eissn: String, lissn: String, openAccess: Boolean) {}
case class HostedByInfo(id: String, officialname: String, journal_id: String, provenance : String, id_type: String) {}

object Aggregators {



  def getId(s1:String, s2:String) : String = {
    if (s1.startsWith("10|")){
      return s1}
    s2
  }

  def getValue(s1:String, s2:String) : String = {
    if(!s1.equals("")){
      return s1
    }
    s2
  }


  def createHostedByItemTypes(df: Dataset[HostedByItemType]): Dataset[HostedByItemType] = {
      val transformedData : Dataset[HostedByItemType] = df
          .groupByKey(_.id)(Encoders.STRING)
        .agg(Aggregators.hostedByAggregator)
          .map{
            case (id:String , res:HostedByItemType) => res
          }(Encoders.product[HostedByItemType])

    transformedData
    }

  val hostedByAggregator: TypedColumn[HostedByItemType, HostedByItemType] = new Aggregator[HostedByItemType, HostedByItemType, HostedByItemType] {
    override def zero: HostedByItemType = HostedByItemType("","","","","",false)
    override def reduce(b: HostedByItemType, a:HostedByItemType): HostedByItemType = {
      return merge(b, a)
    }
    override def merge(b1: HostedByItemType, b2: HostedByItemType): HostedByItemType = {
      if (b1 == null){
        return b2
      }
      if(b2 == null){
        return b1
      }

      HostedByItemType(getId(b1.id, b2.id), getId(b1.officialname, b2.officialname),  getId(b1.issn, b2.issn), getId(b1.eissn, b2.eissn), getId(b1.lissn, b2.lissn), b1.openAccess || b2.openAccess)

    }
    override def finish(reduction: HostedByItemType): HostedByItemType = reduction
    override def bufferEncoder: Encoder[HostedByItemType] = Encoders.product[HostedByItemType]

    override def outputEncoder: Encoder[HostedByItemType] = Encoders.product[HostedByItemType]
  }.toColumn

  def explodeHostedByItemType(df: Dataset[(String, HostedByItemType)]): Dataset[(String, HostedByItemType)] = {
    val transformedData : Dataset[(String, HostedByItemType)] = df
      .groupByKey(_._1)(Encoders.STRING)
      .agg(Aggregators.hostedByAggregator1)
      .map{
        case (id:String , res:(String, HostedByItemType)) => res
      }(Encoders.tuple(Encoders.STRING, Encoders.product[HostedByItemType]))

    transformedData
  }

  val hostedByAggregator1: TypedColumn[(String, HostedByItemType), (String, HostedByItemType)] = new Aggregator[(String, HostedByItemType), (String, HostedByItemType), (String, HostedByItemType)] {
    override def zero: (String, HostedByItemType) = ("", HostedByItemType("","","","","",false))
    override def reduce(b: (String, HostedByItemType), a:(String,HostedByItemType)): (String, HostedByItemType) = {
      return merge(b, a)
    }
    override def merge(b1: (String, HostedByItemType), b2: (String, HostedByItemType)): (String, HostedByItemType) = {
      if (b1 == null){
        return b2
      }
      if(b2 == null){
        return b1
      }
      if(b1._2.id.startsWith("10|")){
        return (b1._1, HostedByItemType(b1._2.id, b1._2.officialname, b1._2.issn, b1._2.eissn, b1._2.lissn, b1._2.openAccess || b2._2.openAccess))

      }
      return (b2._1, HostedByItemType(b2._2.id, b2._2.officialname, b2._2.issn, b2._2.eissn, b2._2.lissn, b1._2.openAccess || b2._2.openAccess))

    }
    override def finish(reduction: (String,HostedByItemType)): (String, HostedByItemType) = reduction
    override def bufferEncoder: Encoder[(String,HostedByItemType)] = Encoders.tuple(Encoders.STRING,Encoders.product[HostedByItemType])

    override def outputEncoder: Encoder[(String,HostedByItemType)] = Encoders.tuple(Encoders.STRING,Encoders.product[HostedByItemType])
  }.toColumn

}
