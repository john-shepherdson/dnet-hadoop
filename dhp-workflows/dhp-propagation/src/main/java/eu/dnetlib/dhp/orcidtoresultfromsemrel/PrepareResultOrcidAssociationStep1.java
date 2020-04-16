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
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

public class PrepareResultOrcidAssociationStep1 {
    private static final Logger log = LoggerFactory.getLogger(PrepareResultOrcidAssociationStep1.class);

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
                    prepareInfo(spark,  inputPath, outputPath, resultClazz, resultType, allowedsemrel);
                });
    }

    private static <R extends Result> void prepareInfo(SparkSession spark,  String inputPath,
                                                       String outputPath, Class<R> resultClazz,
                                                       String resultType,
                                                       List<String> allowedsemrel) {

        //read the relation table and the table related to the result it is using
        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        org.apache.spark.sql.Dataset<Relation> relation = spark.createDataset(sc.textFile(inputPath + "/relation")
                .map(item -> new ObjectMapper().readValue(item, Relation.class)).rdd(), Encoders.bean(Relation.class));
        relation.createOrReplaceTempView("relation");

        log.info("Reading Graph table from: {}", inputPath + "/" + resultType);
        Dataset<R> result = readPathEntity(spark, inputPath + "/" + resultType, resultClazz);

        result.createOrReplaceTempView("result");

        getPossibleResultOrcidAssociation(spark, allowedsemrel, outputPath);

    }

    private static void getPossibleResultOrcidAssociation(SparkSession spark, List<String> allowedsemrel, String outputPath){
        String query = " select target resultId, author authorList" +
                " from (select id, collect_set(named_struct('name', name, 'surname', surname, 'fullname', fullname, 'orcid', orcid)) author " +
                " from ( " +
                " select id, MyT.fullname, MyT.name, MyT.surname, MyP.value orcid " +
                " from result " +
                " lateral view explode (author) a as MyT " +
                " lateral view explode (MyT.pid) p as MyP " +
                " where MyP.qualifier.classid = 'ORCID') tmp " +
                " group by id) r_t " +
                " join (" +
                " select source, target " +
                " from relation " +
                " where datainfo.deletedbyinference = false " +
                getConstraintList(" relclass = '" ,allowedsemrel) + ") rel_rel " +
                " on source = id";

        spark.sql(query)
                .as(Encoders.bean(ResultOrcidList.class))
                .toJSON()
        .write()
        .mode(SaveMode.Append)
        .option("compression","gzip")
        .text(outputPath)
        ;
    }


}
