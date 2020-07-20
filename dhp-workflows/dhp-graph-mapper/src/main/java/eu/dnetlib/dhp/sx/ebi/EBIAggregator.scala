package eu.dnetlib.dhp.sx.ebi
import eu.dnetlib.dhp.schema.oaf.{Publication, Relation, Dataset => OafDataset}
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator



object EBIAggregator {

  def getDatasetAggregator(): Aggregator[(String, OafDataset), OafDataset, OafDataset] = new Aggregator[(String, OafDataset), OafDataset, OafDataset]{

    override def zero: OafDataset = new OafDataset()

    override def reduce(b: OafDataset, a: (String, OafDataset)): OafDataset = {
      b.mergeFrom(a._2)
      if (b.getId == null)
        b.setId(a._2.getId)
      b
    }


    override def merge(wx: OafDataset, wy: OafDataset): OafDataset = {
      wx.mergeFrom(wy)
      if(wx.getId == null && wy.getId.nonEmpty)
        wx.setId(wy.getId)
      wx
    }
    override def finish(reduction: OafDataset): OafDataset = reduction

    override def bufferEncoder: Encoder[OafDataset] =
      Encoders.kryo(classOf[OafDataset])

    override def outputEncoder: Encoder[OafDataset] =
      Encoders.kryo(classOf[OafDataset])
  }


  def getPublicationAggregator(): Aggregator[(String, Publication), Publication, Publication] = new Aggregator[(String, Publication), Publication, Publication]{

    override def zero: Publication = new Publication()

    override def reduce(b: Publication, a: (String, Publication)): Publication = {
      b.mergeFrom(a._2)
      if (b.getId == null)
        b.setId(a._2.getId)
      b
    }


    override def merge(wx: Publication, wy: Publication): Publication = {
      wx.mergeFrom(wy)
      if(wx.getId == null && wy.getId.nonEmpty)
        wx.setId(wy.getId)
      wx
    }
    override def finish(reduction: Publication): Publication = reduction

    override def bufferEncoder: Encoder[Publication] =
      Encoders.kryo(classOf[Publication])

    override def outputEncoder: Encoder[Publication] =
      Encoders.kryo(classOf[Publication])
  }


  def getRelationAggregator(): Aggregator[(String, Relation), Relation, Relation] = new Aggregator[(String, Relation), Relation, Relation]{

    override def zero: Relation = new Relation()

    override def reduce(b: Relation, a: (String, Relation)): Relation = {
      a._2
    }


    override def merge(a: Relation, b: Relation): Relation = {
      if(b!= null) b else a
    }
    override def finish(reduction: Relation): Relation = reduction

    override def bufferEncoder: Encoder[Relation] =
      Encoders.kryo(classOf[Relation])

    override def outputEncoder: Encoder[Relation] =
      Encoders.kryo(classOf[Relation])
  }



}
