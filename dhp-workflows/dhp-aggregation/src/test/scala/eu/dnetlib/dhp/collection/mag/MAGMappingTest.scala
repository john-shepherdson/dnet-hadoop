package eu.dnetlib.dhp.collection.mag

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.schema.oaf.{Dataset, Publication, Result}
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class MAGMappingTest {

  val mapper = new ObjectMapper()

  def mappingTest(): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Test")
      .master("local[*]")
      .getOrCreate()

    val s = new SparkMAGtoOAF(null, null, null)

    s.convertMAG(spark, "/home/sandro/Downloads/mag_test", "/home/sandro/Downloads/mag_oaf")
    s.generateAffiliations(spark, "/home/sandro/Downloads/mag_test", "/home/sandro/Downloads/mag_oaf")

  }

  @Test
  def mappingMagType(): Unit = {

    checkResult[Publication](MagUtility.createResultFromType(null, null), invisible = false, "Other literature type")
    checkResult[Publication](
      MagUtility.createResultFromType(Some("BookChapter"), null),
      invisible = false,
      "Part of book or chapter of book"
    )
    checkResult[Publication](MagUtility.createResultFromType(Some("Book"), null), invisible = false, "Book")
    checkResult[Publication](
      MagUtility.createResultFromType(Some("Repository"), null),
      invisible = true,
      "Other literature type"
    )
    checkResult[Publication](MagUtility.createResultFromType(Some("Thesis"), null), invisible = false, "Thesis")
    checkResult[Publication](MagUtility.createResultFromType(Some("Conference"), null), invisible = false, "Article")
    checkResult[Publication](MagUtility.createResultFromType(Some("Journal"), null), invisible = false, "Journal")
    checkResult[Dataset](MagUtility.createResultFromType(Some("Dataset"), null), invisible = false, "Dataset")
    checkResult[Publication](
      MagUtility.createResultFromType(Some("Patent"), Some("Patent Department of the Navy")),
      invisible = false,
      "Patent"
    )
    checkResult[Publication](
      MagUtility.createResultFromType(Some("Patent"), Some("Brevet Department of the Navy")),
      invisible = false,
      "Patent"
    )
    checkResult[Publication](
      MagUtility.createResultFromType(Some("Patent"), Some("Journal of the Navy")),
      invisible = false,
      "Journal"
    )
    checkResult[Publication](
      MagUtility.createResultFromType(Some("Patent"), Some("Proceedings of the Navy")),
      invisible = false,
      "Article"
    )
    checkResult[Dataset](MagUtility.createResultFromType(Some("Dataset"), null), invisible = false, "Dataset")
    assertNull(MagUtility.createResultFromType(Some("Patent"), null))
    assertNull(MagUtility.createResultFromType(Some("Patent"), Some("Some name ")))
  }

  def checkResult[T](r: Result, invisible: Boolean, typeName: String): Unit = {

    assertNotNull(r)
    assertTrue(r.isInstanceOf[T])
    assertNotNull(r.getDataInfo)
    assertEquals(invisible, r.getDataInfo.getInvisible)
    assertNotNull(r.getInstance())
    assertTrue(r.getInstance().size() > 0)
    assertNotNull(r.getInstance().get(0).getInstancetype)
    assertEquals(typeName, r.getInstance().get(0).getInstancetype.getClassname)

  }

}
