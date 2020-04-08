package eu.dnetlib.dhp.provision;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.DHPUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.Expression;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


/**
 * SparkExtractRelationCount is a spark job that takes in input relation RDD
 * and retrieve for each item in relation which are the number of
 * - Related Dataset
 * - Related Publication
 * - Related Unknown
 */
public class SparkExtractRelationCount {





    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkExtractRelationCount.class.getResourceAsStream("/eu/dnetlib/dhp/provision/input_related_entities_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkExtractRelationCount.class.getSimpleName())
                .master(parser.get("master"))
                .getOrCreate();


        final String workingDirPath = parser.get("workingDirPath");

        final String relationPath = parser.get("relationPath");
        DatasetJoiner.startJoin(spark, relationPath,workingDirPath + "/relatedItemCount");

    }







}
