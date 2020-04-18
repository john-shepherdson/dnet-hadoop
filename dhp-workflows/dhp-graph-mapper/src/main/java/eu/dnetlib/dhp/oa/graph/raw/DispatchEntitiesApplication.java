package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import java.util.Optional;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DispatchEntitiesApplication {

    private static final Logger log = LoggerFactory.getLogger(DispatchEntitiesApplication.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(final String[] args) throws Exception {
        final ArgumentApplicationParser parser =
                new ArgumentApplicationParser(
                        IOUtils.toString(
                                MigrateMongoMdstoresApplication.class.getResourceAsStream(
                                        "/eu/dnetlib/dhp/oa/graph/dispatch_entities_parameters.json")));
        parser.parseArgument(args);

        Boolean isSparkSessionManaged =
                Optional.ofNullable(parser.get("isSparkSessionManaged"))
                        .map(Boolean::valueOf)
                        .orElse(Boolean.TRUE);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        final String sourcePath = parser.get("sourcePath");
        final String targetPath = parser.get("graphRawPath");

        SparkConf conf = new SparkConf();
        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    removeOutputDir(spark, targetPath);

                    processEntity(spark, Publication.class, sourcePath, targetPath);
                    processEntity(spark, Dataset.class, sourcePath, targetPath);
                    processEntity(spark, Software.class, sourcePath, targetPath);
                    processEntity(spark, OtherResearchProduct.class, sourcePath, targetPath);
                    processEntity(spark, Datasource.class, sourcePath, targetPath);
                    processEntity(spark, Organization.class, sourcePath, targetPath);
                    processEntity(spark, Project.class, sourcePath, targetPath);
                    processEntity(spark, Relation.class, sourcePath, targetPath);
                });
    }

    private static <T extends Oaf> void processEntity(
            final SparkSession spark,
            final Class<T> clazz,
            final String sourcePath,
            final String targetPath) {
        final String type = clazz.getSimpleName().toLowerCase();

        log.info(String.format("Processing entities (%s) in file: %s", type, sourcePath));

        /*
        spark.read()
        		.textFile(sourcePath)
        		.filter((FilterFunction<String>) value -> isEntityType(value, type))
        		.map((MapFunction<String, String>) value -> StringUtils.substringAfter(value, "|"), Encoders.STRING())
        		.map((MapFunction<String, T>) value -> OBJECT_MAPPER.readValue(value, clazz), Encoders.bean(clazz))
        		.write()
        		.mode(SaveMode.Overwrite)
        		.parquet(targetPath + "/" + type);

         */

        JavaSparkContext.fromSparkContext(spark.sparkContext())
                .textFile(sourcePath)
                .filter(l -> isEntityType(l, type))
                .map(l -> StringUtils.substringAfter(l, "|"))
                .saveAsTextFile(
                        targetPath + "/" + type, GzipCodec.class); // use repartition(XXX) ???
    }

    private static boolean isEntityType(final String line, final String type) {
        return StringUtils.substringBefore(line, "|").equalsIgnoreCase(type);
    }

    private static void removeOutputDir(SparkSession spark, String path) {
        HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
    }
}
