package eu.dnetlib.dhp.oa.graph.dump;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;

import eu.dnetlib.dhp.schema.dump.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Relation;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Optional;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

public class SparkUpdateProjectInfo implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(SparkUpdateProjectInfo.class);
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String jsonConfiguration = IOUtils
                .toString(
                        SparkUpdateProjectInfo.class
                                .getResourceAsStream(
                                        "/eu/dnetlib/dhp/oa/graph/dump/project_input_parameters.json"));

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
        parser.parseArgument(args);

        Boolean isSparkSessionManaged = Optional
                .ofNullable(parser.get("isSparkSessionManaged"))
                .map(Boolean::valueOf)
                .orElse(Boolean.TRUE);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        final String inputPath = parser.get("sourcePath");
        log.info("inputPath: {}", inputPath);

        final String outputPath = parser.get("outputPath");
        log.info("outputPath: {}", outputPath);

        final String resultClassName = parser.get("resultTableName");
        log.info("resultTableName: {}", resultClassName);


        final String preparedInfoPath = parser.get("preparedInfoPath");
        log.info("preparedInfoPath: {}", preparedInfoPath);

        Class<? extends Result> inputClazz = (Class<? extends Result>) Class.forName(resultClassName);

        SparkConf conf = new SparkConf();

        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    Utils.removeOutputDir(spark, outputPath);
                    extend(spark, inputPath, outputPath , preparedInfoPath,  inputClazz);
                });
    }

    private static <R extends Result > void extend(
            SparkSession spark,
            String inputPath,
            String outputPath,
            String preparedInfoPath,
            Class<R> inputClazz) {

        Dataset<R> result = Utils.readPath(spark, inputPath , inputClazz);
        Dataset<ResultProject> resultProject = Utils.readPath(spark, preparedInfoPath, ResultProject.class);
        result.joinWith(resultProject, result.col("id").equalTo(resultProject.col("resultId")),
                "left")
                .map(value -> {
                    R r = value._1();
                    Optional.ofNullable(value._2()).ifPresent(rp -> {
                        r.setProjects(rp.getProjectsList());
                    });
                    return r;
                },Encoders.bean(inputClazz))
        .write()
        .option("compression","gzip")
        .mode(SaveMode.Overwrite)
        .json(outputPath);


    }

    }
