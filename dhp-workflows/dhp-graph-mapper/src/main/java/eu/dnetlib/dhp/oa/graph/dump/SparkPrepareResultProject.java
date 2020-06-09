package eu.dnetlib.dhp.oa.graph.dump;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.dump.oaf.Projects;
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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

public class SparkPrepareResultProject  implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(SparkPrepareResultProject.class);


    public static void main(String[] args) throws Exception {
        String jsonConfiguration = IOUtils
                .toString(
                        SparkPrepareResultProject.class
                                .getResourceAsStream(
                                        "/eu/dnetlib/dhp/oa/graph/dump/project_prepare_parameters.json"));

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


        SparkConf conf = new SparkConf();

        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    Utils.removeOutputDir(spark, outputPath);
                    prepareResultProjectList(spark, inputPath, outputPath);
                });
    }

    private static void prepareResultProjectList(SparkSession spark, String inputPath, String outputPath) {
        Dataset<Relation> relation = Utils.readPath(spark, inputPath + "/relation" , Relation.class)
                .filter("dataInfo.deletedbyinference = false and relClass = 'produces'");
        Dataset<Project> projects = Utils.readPath(spark, inputPath + "/project" , Project.class);

        projects.joinWith(relation, projects.col("id").equalTo(relation.col("source")))
                .groupByKey((MapFunction<Tuple2<Project,Relation>,String>)value -> value._2().getTarget(), Encoders.STRING())
                .mapGroups((MapGroupsFunction<String, Tuple2<Project,Relation>, ResultProject>) (s, it) ->
                {
                    Tuple2<Project, Relation> first = it.next();
                    ResultProject rp = new ResultProject();
                    rp.setResultId(first._2().getTarget());
                    Project p = first._1();
                    Projects ps = Projects.newInstance(p.getId(), p.getCode().getValue(), p.getAcronym().getValue(),
                            p.getTitle().getValue(), p.getFundingtree()
                                    .stream()
                                    .map(ft -> ft.getValue()).collect(Collectors.toList()));
                    List<Projects>  projList = Arrays.asList(ps);
                    rp.setProjectsList(projList);
                    it.forEachRemaining(c -> {
                        Project op = c._1();
                        projList.add(Projects.newInstance(op.getId(), op.getCode().getValue(),
                                op.getAcronym().getValue(), op.getTitle().getValue(),
                                op.getFundingtree().stream().map(ft -> ft.getValue()).collect(Collectors.toList())));
                    });
                    return rp;
                } ,Encoders.bean(ResultProject.class))
        .write()
        .mode(SaveMode.Overwrite)
        .option("compression", "gzip")
        .json(outputPath);
    }
}
