package eu.dnetlib.dhp.oa.graph.hostebymap

import org.apache.spark.sql.{Dataset, Encoder, Encoders, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator


case class HostedByItemType(id: String, officialname: String, issn: String, eissn: String, lissn: String, openAccess: Boolean) {}


object Aggregators {



  def getId(s1:String, s2:String) : String = {
    if (!s1.equals("")){
      return s1}
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

}
