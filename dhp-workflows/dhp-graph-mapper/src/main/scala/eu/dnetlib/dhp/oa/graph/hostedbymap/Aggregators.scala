package eu.dnetlib.dhp.oa.graph.hostedbymap

import eu.dnetlib.dhp.oa.graph.hostedbymap.model.EntityInfo
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, Encoders, TypedColumn}

case class HostedByItemType(
  id: String,
  officialname: String,
  issn: String,
  eissn: String,
  lissn: String,
  openAccess: Boolean
) {}

case class HostedByInfo(
  id: String,
  officialname: String,
  journal_id: String,
  provenance: String,
  id_type: String
) {}

object Aggregators {

  def getId(s1: String, s2: String): String = {
    if (s1.startsWith("10|")) {
      return s1
    }
    s2
  }

  def getValue(s1: String, s2: String): String = {
    if (!s1.equals("")) {
      return s1
    }
    s2
  }

  def explodeHostedByItemType(
    df: Dataset[(String, HostedByItemType)]
  ): Dataset[(String, HostedByItemType)] = {
    val transformedData: Dataset[(String, HostedByItemType)] = df
      .groupByKey(_._1)(Encoders.STRING)
      .agg(Aggregators.hostedByAggregator)
      .map { case (id: String, res: (String, HostedByItemType)) =>
        res
      }(Encoders.tuple(Encoders.STRING, Encoders.product[HostedByItemType]))

    transformedData
  }

  val hostedByAggregator: TypedColumn[(String, HostedByItemType), (String, HostedByItemType)] =
    new Aggregator[
      (String, HostedByItemType),
      (String, HostedByItemType),
      (String, HostedByItemType)
    ] {

      override def zero: (String, HostedByItemType) =
        ("", HostedByItemType("", "", "", "", "", false))

      override def reduce(
        b: (String, HostedByItemType),
        a: (String, HostedByItemType)
      ): (String, HostedByItemType) = {
        return merge(b, a)
      }

      override def merge(
        b1: (String, HostedByItemType),
        b2: (String, HostedByItemType)
      ): (String, HostedByItemType) = {
        if (b1 == null) {
          return b2
        }
        if (b2 == null) {
          return b1
        }
        if (b1._2.id.startsWith("10|")) {
          return (
            b1._1,
            HostedByItemType(
              b1._2.id,
              b1._2.officialname,
              b1._2.issn,
              b1._2.eissn,
              b1._2.lissn,
              b1._2.openAccess || b2._2.openAccess
            )
          )

        }
        return (
          b2._1,
          HostedByItemType(
            b2._2.id,
            b2._2.officialname,
            b2._2.issn,
            b2._2.eissn,
            b2._2.lissn,
            b1._2.openAccess || b2._2.openAccess
          )
        )

      }

      override def finish(reduction: (String, HostedByItemType)): (String, HostedByItemType) =
        reduction

      override def bufferEncoder: Encoder[(String, HostedByItemType)] =
        Encoders.tuple(Encoders.STRING, Encoders.product[HostedByItemType])

      override def outputEncoder: Encoder[(String, HostedByItemType)] =
        Encoders.tuple(Encoders.STRING, Encoders.product[HostedByItemType])
    }.toColumn

  def resultToSingleIdAggregator: TypedColumn[EntityInfo, EntityInfo] =
    new Aggregator[EntityInfo, EntityInfo, EntityInfo] {
      override def zero: EntityInfo = EntityInfo.newInstance("", "", "")

      override def reduce(b: EntityInfo, a: EntityInfo): EntityInfo = {
        return merge(b, a)
      }

      override def merge(b1: EntityInfo, b2: EntityInfo): EntityInfo = {
        if (b1 == null) {
          return b2
        }
        if (b2 == null) {
          return b1
        }
        if (!b1.getHostedById.equals("")) {
          b1.setOpenAccess(b1.getOpenAccess || b2.getOpenAccess)
          return b1
        }
        b2.setOpenAccess(b1.getOpenAccess || b2.getOpenAccess)
        b2

      }
      override def finish(reduction: EntityInfo): EntityInfo = reduction
      override def bufferEncoder: Encoder[EntityInfo] = Encoders.bean(classOf[EntityInfo])

      override def outputEncoder: Encoder[EntityInfo] = Encoders.bean(classOf[EntityInfo])
    }.toColumn

  def resultToSingleId(df: Dataset[EntityInfo]): Dataset[EntityInfo] = {
    val transformedData: Dataset[EntityInfo] = df
      .groupByKey(_.getId)(Encoders.STRING)
      .agg(Aggregators.resultToSingleIdAggregator)
      .map { case (id: String, res: EntityInfo) =>
        res
      }(Encoders.bean(classOf[EntityInfo]))

    transformedData
  }

  def datasourceToSingleIdAggregator: TypedColumn[EntityInfo, EntityInfo] =
    new Aggregator[EntityInfo, EntityInfo, EntityInfo] {
      override def zero: EntityInfo = EntityInfo.newInstance("", "", "")

      override def reduce(b: EntityInfo, a: EntityInfo): EntityInfo = {
        return merge(b, a)
      }

      override def merge(b1: EntityInfo, b2: EntityInfo): EntityInfo = {
        if (b1 == null) {
          return b2
        }
        if (b2 == null) {
          return b1
        }
        if (!b1.getHostedById.equals("")) {
          return b1
        }
        b2

      }
      override def finish(reduction: EntityInfo): EntityInfo = reduction
      override def bufferEncoder: Encoder[EntityInfo] = Encoders.bean(classOf[EntityInfo])

      override def outputEncoder: Encoder[EntityInfo] = Encoders.bean(classOf[EntityInfo])
    }.toColumn

  def datasourceToSingleId(df: Dataset[EntityInfo]): Dataset[EntityInfo] = {
    val transformedData: Dataset[EntityInfo] = df
      .groupByKey(_.getHostedById)(Encoders.STRING)
      .agg(Aggregators.datasourceToSingleIdAggregator)
      .map { case (id: String, res: EntityInfo) =>
        res
      }(Encoders.bean(classOf[EntityInfo]))

    transformedData
  }
}
