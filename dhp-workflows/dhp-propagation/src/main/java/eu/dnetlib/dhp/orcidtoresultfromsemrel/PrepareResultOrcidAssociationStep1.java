package eu.dnetlib.dhp.orcidtoresultfromsemrel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

public class PrepareResultOrcidAssociation {
    private static final Logger log = LoggerFactory.getLogger(PrepareResultOrcidAssociation.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String jsonConfiguration = IOUtils.toString(SparkOrcidToResultFromSemRelJob3.class
                .getResourceAsStream("/eu/dnetlib/dhp/orcidtoresultfromsemrel/input_prepareorcidtoresult_parameters.json"));

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                jsonConfiguration);

        parser.parseArgument(args);

        Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        String inputPath = parser.get("sourcePath");
        log.info("inputPath: {}", inputPath);

        final String outputPath = parser.get("outputPath");
        log.info("outputPath: {}", outputPath);

        final String resultClassName = parser.get("resultTableName");
        log.info("resultTableName: {}", resultClassName);

        final List<String> allowedsemrel = Arrays.asList(parser.get("allowedsemrel").split(";"));
        log.info("allowedSemRel: {}", new Gson().toJson(allowedsemrel));

        final String resultType = resultClassName.substring(resultClassName.lastIndexOf(".") + 1).toLowerCase();
        log.info("resultType: {}", resultType);


        Class<? extends Result> resultClazz = (Class<? extends Result>) Class.forName(resultClassName);

        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

        runWithSparkHiveSession(conf, isSparkSessionManaged,
                spark -> {
                    if (isTest(parser)) {
                        removeOutputDir(spark, outputPath);
                    }
                    prepareInfo(spark,  inputPath, outputPath, resultClazz, resultType);
                });
    }

    private static <R extends Result> void prepareInfo(SparkSession spark,  String inputPath,
                                                       String outputPath, Class<R> resultClazz,
                                                       String resultType) {

        //read the relation table and the table related to the result it is using
        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        org.apache.spark.sql.Dataset<Relation> relation = spark.createDataset(sc.textFile(inputPath + "/relation")
                .map(item -> new ObjectMapper().readValue(item, Relation.class)).rdd(), Encoders.bean(Relation.class));
        relation.createOrReplaceTempView("relation");

        log.info("Reading Graph table from: {}", inputPath + "/" + resultType);
        Dataset<R> result = readPathEntity(spark, inputPath + "/" + resultType, resultClazz);




    }
}
