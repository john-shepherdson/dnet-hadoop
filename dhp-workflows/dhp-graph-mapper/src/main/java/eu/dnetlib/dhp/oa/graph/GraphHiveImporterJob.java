package eu.dnetlib.dhp.oa.graph;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import java.util.Optional;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphHiveImporterJob {

    private static final Logger log = LoggerFactory.getLogger(GraphHiveImporterJob.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser =
                new ArgumentApplicationParser(
                        IOUtils.toString(
                                GraphHiveImporterJob.class.getResourceAsStream(
                                        "/eu/dnetlib/dhp/oa/graph/input_graph_hive_parameters.json")));
        parser.parseArgument(args);

        Boolean isSparkSessionManaged =
                Optional.ofNullable(parser.get("isSparkSessionManaged"))
                        .map(Boolean::valueOf)
                        .orElse(Boolean.TRUE);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        String inputPath = parser.get("inputPath");
        log.info("inputPath: {}", inputPath);

        String hiveDbName = parser.get("hiveDbName");
        log.info("hiveDbName: {}", hiveDbName);

        String hiveMetastoreUris = parser.get("hiveMetastoreUris");
        log.info("hiveMetastoreUris: {}", hiveMetastoreUris);

        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", hiveMetastoreUris);

        runWithSparkHiveSession(
                conf,
                isSparkSessionManaged,
                spark -> loadGraphAsHiveDB(spark, inputPath, hiveDbName));
    }

    // protected for testing
    private static void loadGraphAsHiveDB(SparkSession spark, String inputPath, String hiveDbName) {

        spark.sql(String.format("DROP DATABASE IF EXISTS %s CASCADE", hiveDbName));
        spark.sql(String.format("CREATE DATABASE IF NOT EXISTS %s", hiveDbName));

        final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        // Read the input file and convert it into RDD of serializable object
        ModelSupport.oafTypes.forEach(
                (name, clazz) ->
                        spark.createDataset(
                                        sc.textFile(inputPath + "/" + name)
                                                .map(s -> OBJECT_MAPPER.readValue(s, clazz))
                                                .rdd(),
                                        Encoders.bean(clazz))
                                .write()
                                .mode(SaveMode.Overwrite)
                                .saveAsTable(hiveDbName + "." + name));
    }
}
