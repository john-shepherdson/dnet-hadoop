package eu.dnetlib.pace.model

import com.jayway.jsonpath.{Configuration, JsonPath}
import eu.dnetlib.pace.config.{DedupConfig, Type}
import eu.dnetlib.pace.util.MapDocumentUtil
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}

import java.util.regex.Pattern
import scala.collection.JavaConverters._

case class SparkModel(conf: DedupConfig) {
  private val URL_REGEX: Pattern = Pattern.compile("^\\s*(http|https|ftp)\\://.*")

  private val CONCAT_REGEX: Pattern = Pattern.compile("\\|\\|\\|")

  val identifierFieldName = "identifier"

  val orderingFieldName = if (!conf.getWf.getOrderField.isEmpty) conf.getWf.getOrderField else identifierFieldName

  val schema: StructType = {
    // create an implicit identifier field
    val identifier = new FieldDef()
    identifier.setName(identifierFieldName)
    identifier.setType(Type.String)

    // Construct a Spark StructType representing the schema of the model
    (Seq(identifier) ++ conf.getPace.getModel.asScala)
      .foldLeft(
        new StructType()
      )((resType, fieldDef) => {
        resType.add(fieldDef.getType match {
          case Type.List | Type.JSON =>
            StructField(fieldDef.getName, DataTypes.createArrayType(DataTypes.StringType), true, Metadata.empty)
          case Type.DoubleArray =>
            StructField(fieldDef.getName, DataTypes.createArrayType(DataTypes.DoubleType), true, Metadata.empty)
          case _ =>
            StructField(fieldDef.getName, DataTypes.StringType, true, Metadata.empty)
        })
      })


  }

  val identityFieldPosition: Int = schema.fieldIndex(identifierFieldName)

  val orderingFieldPosition: Int = schema.fieldIndex(orderingFieldName)

  val parseJsonDataset: (Dataset[String] => Dataset[Row]) = df => {
    df.map(r => rowFromJson(r))(RowEncoder(schema))
  }

  def rowFromJson(json: String): Row = {
    val documentContext =
      JsonPath.using(Configuration.defaultConfiguration.addOptions(com.jayway.jsonpath.Option.SUPPRESS_EXCEPTIONS)).parse(json)
    val values = new Array[Any](schema.size)

    values(identityFieldPosition) = MapDocumentUtil.getJPathString(conf.getWf.getIdPath, documentContext)

    schema.fieldNames.zipWithIndex.foldLeft(values) {
      case ((res, (fname, index))) => {
        val fdef = conf.getPace.getModelMap.get(fname)

        if (fdef != null) {
          res(index) = fdef.getType match {
            case Type.String | Type.Int =>
              MapDocumentUtil.truncateValue(
                MapDocumentUtil.getJPathString(fdef.getPath, documentContext),
                fdef.getLength
              )

            case Type.URL =>
              var uv = MapDocumentUtil.getJPathString(fdef.getPath, documentContext)
              if (!URL_REGEX.matcher(uv).matches)
                uv = ""
              uv

            case Type.List | Type.JSON =>
              MapDocumentUtil.truncateList(
                MapDocumentUtil.getJPathList(fdef.getPath, documentContext, fdef.getType),
                fdef.getSize
              ).asScala

            case Type.StringConcat =>
              val jpaths = CONCAT_REGEX.split(fdef.getPath)

              MapDocumentUtil.truncateValue(
                jpaths
                  .map(jpath => MapDocumentUtil.getJPathString(jpath, documentContext))
                  .mkString(" "),
                fdef.getLength
              )

            case Type.DoubleArray =>
              MapDocumentUtil.getJPathArray(fdef.getPath, json)
          }
        }

        res
      }
    }

    new GenericRowWithSchema(values, schema)
  }
}

