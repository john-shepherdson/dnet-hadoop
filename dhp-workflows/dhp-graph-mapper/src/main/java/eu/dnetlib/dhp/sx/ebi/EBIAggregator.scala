package eu.dnetlib.dhp.sx.ebi
import eu.dnetlib.dhp.schema.oaf.{Publication, Relation, Dataset => OafDataset}
import eu.dnetlib.dhp.schema.scholexplorer.{DLIDataset, DLIPublication, DLIRelation, DLIUnknown}
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



  def getDLIUnknownAggregator(): Aggregator[(String, DLIUnknown), DLIUnknown, DLIUnknown] = new Aggregator[(String, DLIUnknown), DLIUnknown, DLIUnknown]{

    override def zero: DLIUnknown = new DLIUnknown()

    override def reduce(b: DLIUnknown, a: (String, DLIUnknown)): DLIUnknown = {
      b.mergeFrom(a._2)
      if (b.getId == null)
        b.setId(a._2.getId)
      b
    }

    override def merge(wx: DLIUnknown, wy: DLIUnknown): DLIUnknown = {
      wx.mergeFrom(wy)
      if(wx.getId == null && wy.getId.nonEmpty)
        wx.setId(wy.getId)
      wx
    }
    override def finish(reduction: DLIUnknown): DLIUnknown = reduction

    override def bufferEncoder: Encoder[DLIUnknown] =
      Encoders.kryo(classOf[DLIUnknown])

    override def outputEncoder: Encoder[DLIUnknown] =
      Encoders.kryo(classOf[DLIUnknown])
  }

  def getDLIDatasetAggregator(): Aggregator[(String, DLIDataset), DLIDataset, DLIDataset] = new Aggregator[(String, DLIDataset), DLIDataset, DLIDataset]{

    override def zero: DLIDataset = new DLIDataset()

    override def reduce(b: DLIDataset, a: (String, DLIDataset)): DLIDataset = {
      b.mergeFrom(a._2)
      if (b.getId == null)
        b.setId(a._2.getId)
      b
    }

    override def merge(wx: DLIDataset, wy: DLIDataset): DLIDataset = {
      wx.mergeFrom(wy)
      if(wx.getId == null && wy.getId.nonEmpty)
        wx.setId(wy.getId)
      wx
    }
    override def finish(reduction: DLIDataset): DLIDataset = reduction

    override def bufferEncoder: Encoder[DLIDataset] =
      Encoders.kryo(classOf[DLIDataset])

    override def outputEncoder: Encoder[DLIDataset] =
      Encoders.kryo(classOf[DLIDataset])
  }


  def getDLIPublicationAggregator(): Aggregator[(String, DLIPublication), DLIPublication, DLIPublication] = new Aggregator[(String, DLIPublication), DLIPublication, DLIPublication]{

    override def zero: DLIPublication = new DLIPublication()

    override def reduce(b: DLIPublication, a: (String, DLIPublication)): DLIPublication = {
      b.mergeFrom(a._2)
      if (b.getId == null)
        b.setId(a._2.getId)
      b
    }


    override def merge(wx: DLIPublication, wy: DLIPublication): DLIPublication = {
      wx.mergeFrom(wy)
      if(wx.getId == null && wy.getId.nonEmpty)
        wx.setId(wy.getId)
      wx
    }
    override def finish(reduction: DLIPublication): DLIPublication = reduction

    override def bufferEncoder: Encoder[DLIPublication] =
      Encoders.kryo(classOf[DLIPublication])

    override def outputEncoder: Encoder[DLIPublication] =
      Encoders.kryo(classOf[DLIPublication])
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


  def getDLIRelationAggregator(): Aggregator[(String, DLIRelation), DLIRelation, DLIRelation] = new Aggregator[(String, DLIRelation), DLIRelation, DLIRelation]{

    override def zero: DLIRelation = new DLIRelation()

    override def reduce(b: DLIRelation, a: (String, DLIRelation)): DLIRelation = {
      a._2
    }


    override def merge(a: DLIRelation, b: DLIRelation): DLIRelation = {
      if(b!= null) b else a
    }
    override def finish(reduction: DLIRelation): DLIRelation = reduction

    override def bufferEncoder: Encoder[DLIRelation] =
      Encoders.kryo(classOf[DLIRelation])

    override def outputEncoder: Encoder[DLIRelation] =
      Encoders.kryo(classOf[DLIRelation])
  }



}
