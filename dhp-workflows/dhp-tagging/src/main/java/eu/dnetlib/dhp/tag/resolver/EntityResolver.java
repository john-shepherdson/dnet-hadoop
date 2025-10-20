package eu.dnetlib.dhp.tag.resolver;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface EntityResolver {
    Dataset<Row> resolve(SparkSession spark, String graphDatabase, String leftEntity, String rightEntity, String relation);
}
