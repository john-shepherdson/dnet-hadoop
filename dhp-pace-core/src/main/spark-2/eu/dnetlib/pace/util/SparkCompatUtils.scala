package eu.dnetlib.pace.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.StructType

object SparkCompatUtils {

  def encoderFor(schema: StructType): ExpressionEncoder[Row] = {
    RowEncoder(schema)
  }
}