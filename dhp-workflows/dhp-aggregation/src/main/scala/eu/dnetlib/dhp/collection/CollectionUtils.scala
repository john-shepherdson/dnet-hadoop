package eu.dnetlib.dhp.collection

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.schema.common.ModelSupport
import eu.dnetlib.dhp.schema.oaf.{Oaf, OafEntity, Relation}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SaveMode}

object CollectionUtils {

  /** This method in pipeline to the transformation phase,
    * generates relations in both verse, typically it should be a phase of flatMap
    *
    * @param i input OAF
    * @return
    * If the input OAF is an entity -> List(i)
    * If the input OAF is a relation -> List(relation, inverseRelation)
    */

  def fixRelations(i: Oaf): List[Oaf] = {
    if (i.isInstanceOf[OafEntity])
      return List(i)
    else {
      val r: Relation = i.asInstanceOf[Relation]
      val currentRel = ModelSupport.findRelation(r.getRelClass)
      if (currentRel != null) {

        // Cleaning relation
        r.setRelType(currentRel.getRelType)
        r.setSubRelType(currentRel.getSubReltype)
        r.setRelClass(currentRel.getRelClass)
        val inverse = new Relation
        inverse.setSource(r.getTarget)
        inverse.setTarget(r.getSource)
        inverse.setRelType(currentRel.getRelType)
        inverse.setSubRelType(currentRel.getSubReltype)
        inverse.setRelClass(currentRel.getInverseRelClass)
        inverse.setCollectedfrom(r.getCollectedfrom)
        inverse.setDataInfo(r.getDataInfo)
        inverse.setProperties(r.getProperties)
        inverse.setLastupdatetimestamp(r.getLastupdatetimestamp)
        inverse.setValidated(r.getValidated)
        inverse.setValidationDate(r.getValidationDate)
        return List(r, inverse)
      }
    }
    List()
  }

  def saveDataset(dataset: Dataset[Oaf], targetPath: String): Unit = {
    implicit val resultEncoder: Encoder[Oaf] = Encoders.kryo(classOf[Oaf])
    val mapper = new ObjectMapper

    dataset
      .flatMap(i => CollectionUtils.fixRelations(i))
      .filter(i => i != null)
      .map(r => mapper.writeValueAsString(r))(Encoders.STRING)
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "gzip")
      .text(targetPath)
  }

}
