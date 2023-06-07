package eu.dnetlib.dhp.oa.dedup.dsl

import org.apache.spark.sql.types.StructType

case class Deduper (schema: StructType,
               clusterings: Seq[Clustering] = Seq()) {

  def withClustering(clusterings: Clustering*) =
    copy(clusterings = clusterings)

}
