package eu.dnetlib.dhp.graph;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkGraphImporterJob {

    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(SparkGraphImporterJob.class.getResourceAsStream(
                        "/eu/dnetlib/dhp/graph/input_graph_parameters.json")));
        parser.parseArgument(args);

        try(SparkSession spark = getSparkSession(parser)) {

            final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
            final String inputPath = parser.get("sourcePath");
            final String hiveDbName = parser.get("hive_db_name");

            spark.sql(String.format("DROP DATABASE IF EXISTS %s CASCADE", hiveDbName));
            spark.sql(String.format("CREATE DATABASE IF NOT EXISTS %s", hiveDbName));

            // Read the input file and convert it into RDD of serializable object
            GraphMappingUtils.types.forEach((name, clazz) -> spark.createDataset(sc.textFile(inputPath + "/" + name)
                    .map(s -> new ObjectMapper().readValue(s, clazz))
                    .rdd(), Encoders.bean(clazz))
                    .write()
                    .mode(SaveMode.Overwrite)
                    .saveAsTable(hiveDbName + "." + name));
        }
    }

    private static SparkSession getSparkSession(ArgumentApplicationParser parser) {
        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

        return SparkSession
                .builder()
                .appName(SparkGraphImporterJob.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();
    }
}
