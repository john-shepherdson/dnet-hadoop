package eu.dnetlib.dhp.resulttocommunityfromsemrel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import eu.dnetlib.dhp.QueryInformationSystem;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.resulttocommunityfromorganization.ResultCommunityList;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkHiveSession;

public class PrepareResultCommunitySetStep1 {
    private static final Logger log = LoggerFactory.getLogger(PrepareResultCommunitySetStep1.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String jsonConfiguration = IOUtils.toString(PrepareResultCommunitySetStep1.class
                .getResourceAsStream("/eu/dnetlib/dhp/resulttocommunityfromsemrel/input_preparecommunitytoresult_parameters.json"));

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

        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));

        final List<String> allowedsemrel = Arrays.asList(parser.get("allowedsemrels").split(";"));
        log.info("allowedSemRel: {}", new Gson().toJson(allowedsemrel));

        final String isLookupUrl = parser.get("isLookupUrl");
        log.info("isLookupUrl: {}", isLookupUrl);

        final List<String> communityIdList = QueryInformationSystem.getCommunityList(isLookupUrl);
        log.info("communityIdList: {}", new Gson().toJson(communityIdList));

        final String resultType = resultClassName.substring(resultClassName.lastIndexOf(".") + 1).toLowerCase();
        log.info("resultType: {}", resultType);


        Class<? extends Result> resultClazz = (Class<? extends Result>) Class.forName(resultClassName);


        runWithSparkHiveSession(conf, isSparkSessionManaged,
                spark -> {
                    if (isTest(parser)) {
                        removeOutputDir(spark, outputPath);
                    }
                    prepareInfo(spark,  inputPath, outputPath, allowedsemrel, resultClazz, resultType,
                            communityIdList);
                });
    }

    private static <R extends Result> void prepareInfo(SparkSession spark, String inputPath, String outputPath,
                                                       List<String> allowedsemrel, Class<R> resultClazz, String resultType,
                                                       List<String> communityIdList) {
        //read the relation table and the table related to the result it is using
        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        org.apache.spark.sql.Dataset<Relation> relation = spark.createDataset(sc.textFile(inputPath + "/relation")
                .map(item -> OBJECT_MAPPER.readValue(item, Relation.class)).rdd(), Encoders.bean(Relation.class));
        relation.createOrReplaceTempView("relation");

        log.info("Reading Graph table from: {}", inputPath + "/" + resultType);
        Dataset<R> result = readPathEntity(spark, inputPath + "/" + resultType, resultClazz);

        result.createOrReplaceTempView("result");

        getPossibleResultcommunityAssociation(spark, allowedsemrel, outputPath + "/" + resultType, communityIdList);

    }

    private static void getPossibleResultcommunityAssociation(SparkSession spark, List<String> allowedsemrel, String outputPath,
                                                              List<String> communityIdList) {

        String communitylist = getConstraintList(" co.id = '", communityIdList);
        String semrellist = getConstraintList(" relClass = '", allowedsemrel );


        /*
        associates to each result the set of community contexts they are associated to
        select id, collect_set(co.id) community_context " +
                "       from  result " +
                "       lateral view explode (context) c as co " +
                "       where datainfo.deletedbyinference = false "+ communitylist +
                "       group by id

        associates to each target of a relation with allowed semantics the set of community context it could possibly
        inherit from the source of the relation
         */
        String query = "Select target resultId, community_context  " +
                "from (select id, collect_set(co.id) community_context " +
                "       from  result " +
                "       lateral view explode (context) c as co " +
                "       where datainfo.deletedbyinference = false "+ communitylist +
                "       group by id) p " +
                "JOIN " +
                "(select source, target " +
                "from relation " +
                "where datainfo.deletedbyinference = false " + semrellist + ") r " +
                "ON p.id = r.source";

        org.apache.spark.sql.Dataset<Row> result_context = spark.sql( query);
        result_context.createOrReplaceTempView("result_context");

        //( target, (mes, dh-ch-, ni))
        /*
        a dataset for example could be linked to more than one publication. For each publication linked to that dataset
        the previous query will produce a row: targetId set of community context the te=arget could possibly inherit
        with the following query there will be a single row for each result linked to more than one result of the result type
        currently being used
         */
        query = "select resultId , collect_set(co) communityList " +
                "from result_context " +
                "lateral view explode (community_context) c as co " +
                "where length(co) > 0 " +
                "group by resultId";

        spark.sql(query)
                .as(Encoders.bean(ResultCommunityList.class))
                .toJavaRDD()
                .map(r -> OBJECT_MAPPER.writeValueAsString(r))
                .saveAsTextFile(outputPath, GzipCodec.class);
    }
}
