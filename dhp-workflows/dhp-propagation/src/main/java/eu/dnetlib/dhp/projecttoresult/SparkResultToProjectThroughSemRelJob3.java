package eu.dnetlib.dhp.projecttoresult;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.countrypropagation.PrepareDatasourceCountryAssociation;
import eu.dnetlib.dhp.schema.oaf.Relation;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

public class SparkResultToProjectThroughSemRelJob3 {

    private static final Logger log = LoggerFactory.getLogger(PrepareDatasourceCountryAssociation.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        String jsonConfiguration = IOUtils.toString(SparkResultToProjectThroughSemRelJob3.class
                .getResourceAsStream("/eu/dnetlib/dhp/projecttoresult/input_projecttoresult_parameters.json"));

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                jsonConfiguration);

        parser.parseArgument(args);

        Boolean isSparkSessionManaged = isSparkSessionManaged(parser);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        final String outputPath = parser.get("outputPath");
        log.info("outputPath {}: ", outputPath);

        final String potentialUpdatePath = parser.get("potentialUpdatePath");
        log.info("potentialUpdatePath {}: ", potentialUpdatePath);

        final String alreadyLinkedPath = parser.get("alreadyLinkedPath");
        log.info("alreadyLinkedPath {}: ", alreadyLinkedPath);

        final Boolean writeUpdates = Boolean.valueOf(parser.get("writeUpdate"));
        log.info("writeUpdate: {}", writeUpdates);

        final Boolean saveGraph = Boolean.valueOf(parser.get("saveGraph"));
        log.info("saveGraph: {}", saveGraph);

        SparkConf conf = new SparkConf();


        runWithSparkSession(conf, isSparkSessionManaged,
                spark -> {
                    if(isTest(parser)) {
                        removeOutputDir(spark, outputPath);
                    }
                    execPropagation(spark, outputPath, alreadyLinkedPath, potentialUpdatePath, writeUpdates, saveGraph);
                });
    }


    private static void execPropagation(SparkSession spark, String outputPath, String alreadyLinkedPath, String potentialUpdatePath,
                                 Boolean writeUpdate, Boolean saveGraph){

        Dataset<ProjectResultSet> toaddrelations = readAssocProjectResults(spark, potentialUpdatePath);
        Dataset<ProjectResultSet> alreadyLinked =  readAssocProjectResults(spark, alreadyLinkedPath);

        if(writeUpdate){
            toaddrelations
                    .toJSON()
                    .write()
                    .mode(SaveMode.Overwrite)
                    .option("compression","gzip")
                    .text(outputPath +"/potential_updates");
        }
        if (saveGraph){
            getNewRelations(alreadyLinked, toaddrelations)
                    .toJSON()
                    .write()
                    .mode(SaveMode.Append)
                    .option("compression", "gzip")
                    .text(outputPath);

        }

    }

    private static Dataset<Relation> getNewRelations(Dataset<ProjectResultSet> alreadyLinked,
                                              Dataset<ProjectResultSet> toaddrelations){


        return toaddrelations
                .joinWith(alreadyLinked, toaddrelations.col("projectId").equalTo(alreadyLinked.col("projectId")), "left")
                .flatMap(value -> {
                    List<Relation> new_relations = new ArrayList<>();
                    ProjectResultSet potential_update = value._1();
                    ProjectResultSet already_linked = value._2();
                    String projId = already_linked.getProjectId();
                    potential_update
                            .getResultSet()
                            .stream()
                            .forEach(rId -> {
                                if (!already_linked.getResultSet().contains(rId)){
                                    new_relations.add(getRelation(rId, projId, RELATION_RESULT_PROJECT_REL_CLASS, RELATION_RESULTPROJECT_REL_TYPE,
                                            RELATION_RESULTPROJECT_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
                                            PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_ID,
                                            PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_NAME));
                                    new_relations.add(getRelation(projId, rId, RELATION_PROJECT_RESULT_REL_CLASS, RELATION_RESULTPROJECT_REL_TYPE,
                                            RELATION_RESULTPROJECT_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
                                            PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_ID,
                                            PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_NAME));
                                }
                            });
                    return new_relations.iterator();

                        }
                ,Encoders.bean(Relation.class));




        }

    private static Dataset<ProjectResultSet> readAssocProjectResults(SparkSession spark, String potentialUpdatePath) {
        return spark
                .read()
                .textFile(potentialUpdatePath)
                .map(value -> OBJECT_MAPPER.readValue(value, ProjectResultSet.class), Encoders.bean(ProjectResultSet.class));
    }

}
