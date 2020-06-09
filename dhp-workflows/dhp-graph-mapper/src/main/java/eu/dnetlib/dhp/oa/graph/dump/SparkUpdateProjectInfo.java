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
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

public class UpdateProjectInfo implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(UpdateProjectInfo.class);
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String jsonConfiguration = IOUtils
                .toString(
                        UpdateProjectInfo.class
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

        final String resultType = parser.get("resultType");
        log.info("resultType: {}", resultType);

        Class<? extends Result> inputClazz = (Class<? extends Result>) Class.forName(resultClassName);

        SparkConf conf = new SparkConf();

        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    Utils.removeOutputDir(spark, outputPath);
                    extend(spark, inputPath, outputPath , resultType,  inputClazz);
                });
    }

    private static <R extends Result > void extend(
            SparkSession spark,
            String inputPath,
            String outputPath,
            String resultType,
            Class<R> inputClazz) {

        Dataset<R> result = Utils.readPath(spark, inputPath + "/" + resultType, inputClazz);
        Dataset<Relation> relation = Utils.readPath(spark, inputPath + "/relation", Relation.class)
                .filter("dataInfo.deletedbyinference = false and relClass = 'produces'");
        Dataset<Project> project = Utils.readPath(spark,inputPath + "/project", Project.class);
        relation.joinWith(project, relation.col("source").equalTo(project.col("id")))
        result.joinWith(relation, result.col("id").equalTo(relation.col("target")), "left")
                .groupByKey(
                        (MapFunction<Tuple2<R,Relation>, String>) p -> p._1().getId(),
                        Encoders.STRING())
                .mapGroups((MapGroupsFunction<String, Tuple2<R, Relation>, R>)(c, it) -> {
                    Tuple2<R, Relation> first = it.next();


                }, Encoders.bean(inputClazz));
                .mapGroups((MapGroupsFunction<String, Project, Project>) (s, it) -> {
                    Project first = it.next();
                    it.forEachRemaining(p -> {
                        first.mergeFrom(p);
                    });
                    return first;
                }



    }

    }
