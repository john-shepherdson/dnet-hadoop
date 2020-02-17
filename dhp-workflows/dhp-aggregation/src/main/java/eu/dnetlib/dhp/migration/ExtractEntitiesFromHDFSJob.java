package eu.dnetlib.dhp.migration;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class ExtractEntitiesFromHDFSJob {


    private static List<String> folderNames = Arrays.asList("db_entities", "oaf_entities", "odf_entities");

    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(MigrateMongoMdstoresApplication.class.getResourceAsStream("/eu/dnetlib/dhp/migration/extract_entities_from_hdfs_parameters.json")));
        parser.parseArgument(args);

        final SparkSession spark = SparkSession
                .builder()
                .appName(ExtractEntitiesFromHDFSJob.class.getSimpleName())
                .master(parser.get("master"))
                .getOrCreate();

        final String sourcePath = parser.get("sourcePath");
        final String targetPath = parser.get("graphRawPath");
        final String entity = parser.get("entity");


        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());


        JavaRDD<String> inputRdd = sc.emptyRDD();


        folderNames.forEach(p -> inputRdd.union(
                sc.sequenceFile(sourcePath+"/"+p, Text.class, Text.class)
                    .map(k -> new Tuple2<>(k._1().toString(), k._2().toString()))
                        .filter(k -> isEntityType(k._1(), entity))
                        .map(Tuple2::_2))
        );

        inputRdd.saveAsTextFile(targetPath+"/"+entity);
    }


    private static boolean isEntityType(final String item, final String entity) {
        return StringUtils.substringAfter(item, ":").equalsIgnoreCase(entity);
    }
}
