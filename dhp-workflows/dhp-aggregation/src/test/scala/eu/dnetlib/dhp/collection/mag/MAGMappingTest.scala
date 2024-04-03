package eu.dnetlib.dhp.collection.mag

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.schema.oaf.{Dataset, Publication, Result}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._



class MAGMappingTest {

  val mapper = new ObjectMapper()


  def mappingTest(): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Test")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val magDS = spark.read.load("/home/sandro/Downloads/mag").as[MAGPaper].where(col("journalId").isNotNull)

    val paper = magDS.first()

    print(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(MagUtility.convertMAGtoOAF(paper)))

  }



  @Test
  def mappingMagType(): Unit = {
    /*
    +-----------+--------+
    |    docType|   count|
    +-----------+--------+
    |       null|79939635|
    |BookChapter| 2431452|
    |    Dataset|  123923|
    | Repository| 5044165|
    |     Thesis| 5525681|
    | Conference| 5196866|
    |    Journal|89452763|
    |       Book| 4460017|
    |     Patent|64631955|
    +-----------+--------+

    "instancetype":{
    "classid":"0001",
    "classname":"Article",
    "schemeid":"dnet:publication_resource",
    "schemename":"dnet:publication_resource"},"instanceTypeMapping":[{"originalType":"journal-article","typeCode":null,"typeLabel":null,"vocabularyName":"openaire::coar_resource_types_3_1"}

 */

    checkResult[Publication](MagUtility.getInstanceType(null, null), invisible = false,"Other literature type")
    checkResult[Publication](MagUtility.getInstanceType(Some("BookChapter"), null), invisible = false,"Part of book or chapter of book")
    checkResult[Publication](MagUtility.getInstanceType(Some("Book"), null), invisible = false,"Book")
    checkResult[Publication](MagUtility.getInstanceType(Some("Repository"), null), invisible = true,"Other literature type")
    checkResult[Publication](MagUtility.getInstanceType(Some("Thesis"), null), invisible = false,"Thesis")
    checkResult[Publication](MagUtility.getInstanceType(Some("Conference"), null), invisible = false,"Article")
    checkResult[Publication](MagUtility.getInstanceType(Some("Journal"), null), invisible = false,"Journal")
    checkResult[Dataset](MagUtility.getInstanceType(Some("Dataset"), null), invisible = false,"Dataset")
    checkResult[Publication](MagUtility.getInstanceType(Some("Patent"), Some("Patent Department of the Navy")), invisible = false,"Patent")
    checkResult[Dataset](MagUtility.getInstanceType(Some("Dataset"), null), invisible = false,"Dataset")



  }


  def  checkResult[T](r:Result, invisible:Boolean, typeName:String): Unit = {

    assertNotNull(r)
    assertTrue(r.isInstanceOf[T])
    assertNotNull(r.getDataInfo)
    assertEquals( invisible ,r.getDataInfo.getInvisible)
    assertNotNull(r.getInstance())
    assertTrue(r.getInstance().size()>0)
    assertNotNull(r.getInstance().get(0).getInstancetype)
    assertEquals(typeName, r.getInstance().get(0).getInstancetype.getClassname)

  }

}
