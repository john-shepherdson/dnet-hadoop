package eu.dnetlib.dhp.oa.dedup.graph

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions;

object GraphProcessor {

  def findCCs(vertexes: RDD[(VertexId, String)], edges: RDD[Edge[String]], subEntity: String, maxIterations: Int, cut:Int): RDD[ConnectedComponent] = {
    val graph: Graph[String, String] = Graph(vertexes, edges).partitionBy(PartitionStrategy.RandomVertexCut) //TODO remember to remove partitionby
    val cc = graph.connectedComponents(maxIterations).vertices

    val joinResult = vertexes.leftOuterJoin(cc).map {
      case (id, (openaireId, cc)) => {
        if (cc.isEmpty) {
          (id, openaireId)
        }
        else {
          (cc.get, openaireId)
        }
      }
    }
    val connectedComponents = joinResult.groupByKey()
      .map[ConnectedComponent](cc => asConnectedComponent(cc, subEntity, cut))
    connectedComponents
  }



  def asConnectedComponent(group: (VertexId, Iterable[String]), subEntity: String,cut:Int): ConnectedComponent = {
    val docs = group._2.toSet[String]
    val connectedComponent = new ConnectedComponent(JavaConversions.setAsJavaSet[String](docs), subEntity, cut);
    connectedComponent
  }

}