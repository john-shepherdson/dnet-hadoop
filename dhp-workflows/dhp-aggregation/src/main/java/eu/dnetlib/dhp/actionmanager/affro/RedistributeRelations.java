package eu.dnetlib.dhp.actionmanager.affro;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.actionmanager.Constants;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static eu.dnetlib.dhp.actionmanager.affro.Constants.*;
import static org.apache.spark.sql.functions.*;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.collect_list;

public class RedistributeRelations implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(PrepareDataset.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        String jsonConfiguration = IOUtils
                .toString(
                        PrepareDataset.class
                                .getResourceAsStream(
                                        "/eu/dnetlib/dhp/actionmanager/affro/input_redistribute_relations_parameter.json"));
        log.info("read parameter file");

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
        parser.parseArgument(args);

        Boolean isSparkSessionManaged = eu.dnetlib.dhp.actionmanager.Constants.isSparkSessionManaged(parser);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        final String explodedPath = parser.get("explodedResultPath");
        log.info("explodedPath: {}", explodedPath);

        final String matchingsPath = parser.get("matchingsPath");
        log.info("matchingsPath: {}", matchingsPath);

        final String outputPath = parser.get("outputPath");
        log.info("outputPath: {}", outputPath);

        final String workingDir = parser.get("workingDir");
        log.info("workingDir: {}", workingDir);

        SparkConf conf = new SparkConf();

        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    Constants.removeOutputDir(spark, outputPath);
                    redistributeRelations(
                            spark, explodedPath, matchingsPath, outputPath, workingDir);
                });
    }

    private static void redistributeRelations(SparkSession spark, String explodedPath, String matchingsPath, String outputPath, String workingDir) {
        spark
                .udf()
                .register(
                        "insertKey", (String id, String fullname) -> StringUtils.isNotEmpty(fullname) ? id + fullname : id, DataTypes.StringType);

        spark.udf().register(
                "aggregateAuthor",
                new AggregateAuthorUDF(),
                AUTHOR_AGGREGATED_SCHEMA
        );

        spark.udf().register(
                "aggregateResult",
                new AggregateResultUDF(),
                RESULT_MATCHED_SCHEMA
        );

        Dataset<Row> exploded = spark.read().schema(eu.dnetlib.dhp.actionmanager.affro.Constants.DATASET_SCHEMA)
                .json(explodedPath);

        Dataset<Row> matchings = spark.read().schema(eu.dnetlib.dhp.actionmanager.affro.Constants.AFFILIATION_SCHEMA)
                .json(matchingsPath);

        Dataset<Row> joined = exploded.join(matchings, exploded.col("raw_affiliation_string").equalTo(matchings.col("Affiliation")))
                .filter(col("Matchings").isNotNull().and(size(col("Matchings")).gt(0)))
                .select("id", "fullname", "raw_affiliation_string", "Matchings")
                .withColumn("key", expr("insertKey(id, fullname)"));

        Dataset<Row> groupedDf = joined
                .groupBy("key")
                .agg(collect_list(struct(joined.col("*"))).alias("group"))
                .withColumn("aggAuthor", expr("aggregateAuthor(group)"))
                .select("aggAuthor.*");

        Dataset<Row> resultDf = groupedDf
                .groupBy("id")
                .agg(collect_list(struct(groupedDf.col("*"))).alias("group"))
                .withColumn("result", expr("aggregateResult(group)"))
                .select("result.*");

        resultDf.write().mode(SaveMode.Overwrite).option("compression","gzip").json(outputPath);
                

    }


}


