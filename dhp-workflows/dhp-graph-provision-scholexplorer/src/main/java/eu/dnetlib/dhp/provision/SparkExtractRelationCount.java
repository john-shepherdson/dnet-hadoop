package eu.dnetlib.dhp.provision;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.*;

/**
 * SparkExtractRelationCount is a spark job that takes in input relation RDD and retrieve for each
 * item in relation which are the number of - Related Dataset - Related Publication - Related
 * Unknown
 */
public class SparkExtractRelationCount {

  public static void main(String[] args) throws Exception {
    final ArgumentApplicationParser parser =
        new ArgumentApplicationParser(
            IOUtils.toString(
                SparkExtractRelationCount.class.getResourceAsStream(
                    "/eu/dnetlib/dhp/provision/input_related_entities_parameters.json")));
    parser.parseArgument(args);
    final SparkSession spark =
        SparkSession.builder()
            .appName(SparkExtractRelationCount.class.getSimpleName())
            .master(parser.get("master"))
            .getOrCreate();

    final String workingDirPath = parser.get("workingDirPath");

    final String relationPath = parser.get("relationPath");
    DatasetJoiner.startJoin(spark, relationPath, workingDirPath + "/relatedItemCount");
  }
}
