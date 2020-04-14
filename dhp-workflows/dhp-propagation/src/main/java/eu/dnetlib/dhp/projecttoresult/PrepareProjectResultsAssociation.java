package eu.dnetlib.dhp.projecttoresult;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.countrypropagation.PrepareDatasourceCountryAssociation;
import eu.dnetlib.dhp.schema.oaf.Relation;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.PropagationConstant.getConstraintList;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

public class PrepareProjectResultsAssociation {
    private static final Logger log = LoggerFactory.getLogger(PrepareDatasourceCountryAssociation.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception{

        String jsonConfiguration = IOUtils.toString(PrepareProjectResultsAssociation.class
                .getResourceAsStream("/eu/dnetlib/dhp/projecttoresult/input_prepareprojecttoresult_parameters.json"));

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                jsonConfiguration);

        parser.parseArgument(args);

        Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        String inputPath = parser.get("sourcePath");
        log.info("inputPath: {}", inputPath);

        final String potentialUpdatePath = parser.get("potentialUpdatePath");
        log.info("potentialUpdatePath {}: ", potentialUpdatePath);

        String alreadyLinkedPath = parser.get("alreadyLinkedPath");
        log.info("alreadyLinkedPath: {} ", alreadyLinkedPath);

        final List<String> allowedsemrel = Arrays.asList(parser.get("allowedsemrels").split(";"));
        log.info("allowedSemRel: {}", new Gson().toJson(allowedsemrel));

        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

        runWithSparkHiveSession(conf, isSparkSessionManaged,
                spark -> {
//                    removeOutputDir(spark, potentialUpdatePath);
//                    removeOutputDir(spark, alreadyLinkedPath);
                    prepareResultProjProjectResults(spark, inputPath, potentialUpdatePath, alreadyLinkedPath, allowedsemrel);

                });
    }

    private static void prepareResultProjProjectResults(SparkSession spark, String inputPath, String potentialUpdatePath,
                                                        String alreadyLinkedPath, List<String> allowedsemrel) {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        Dataset<Relation> relation = spark.createDataset(sc.textFile(inputPath )
                .map(item -> OBJECT_MAPPER.readValue(item, Relation.class)).rdd(), Encoders.bean(Relation.class));

        relation.createOrReplaceTempView("relation");

        String query = "SELECT source, target " +
                "       FROM relation " +
                "       WHERE datainfo.deletedbyinference = false " +
                "       AND relClass = '" + RELATION_RESULT_PROJECT_REL_CLASS + "'";

        Dataset<Row> resproj_relation = spark.sql(query);
        resproj_relation.createOrReplaceTempView("resproj_relation");

        query ="SELECT projectId, collect_set(resId) resultSet " +
                "FROM (" +
                "      SELECT r1.target resId, r2.target projectId " +
                "      FROM (SELECT source, target " +
                "            FROM relation " +
                "            WHERE datainfo.deletedbyinference = false  " +
                                   getConstraintList(" relClass = '", allowedsemrel )  + ") r1" +
                "      JOIN resproj_relation r2 " +
                "      ON r1.source = r2.source " +
                "      ) tmp " +
                "GROUP BY projectId ";

        spark.sql(query).as(Encoders.bean(ProjectResultSet.class))
                .toJSON()
                .write()
                .mode(SaveMode.Overwrite)
                .option("compression","gzip")
                .text(potentialUpdatePath);
//                .toJavaRDD()
//                .map(r -> OBJECT_MAPPER.writeValueAsString(r))
//                .saveAsTextFile(potentialUpdatePath, GzipCodec.class);


        query = "SELECT target projectId, collect_set(source) resultSet " +
                "FROM resproj_relation " +
                "GROUP BY target";

        spark.sql(query)
                .as(Encoders.bean(ProjectResultSet.class))
                .toJSON()
                .write()
                .mode(SaveMode.Overwrite)
                .option("compression","gzip")
                .text(alreadyLinkedPath);
//                .toJavaRDD()
//                .map(r -> OBJECT_MAPPER.writeValueAsString(r))
//                .saveAsTextFile(alreadyLinkedPath, GzipCodec.class);


    }
}
