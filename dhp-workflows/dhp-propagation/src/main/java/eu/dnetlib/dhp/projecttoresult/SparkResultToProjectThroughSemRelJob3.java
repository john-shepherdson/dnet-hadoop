package eu.dnetlib.dhp.projecttoresult;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.countrypropagation.DatasourceCountry;
import eu.dnetlib.dhp.countrypropagation.PrepareDatasourceCountryAssociation;
import eu.dnetlib.dhp.countrypropagation.ResultCountrySet;
import eu.dnetlib.dhp.schema.oaf.Relation;
import org.apache.arrow.flatbuf.Bool;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.omg.CORBA.OBJ_ADAPTER;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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

        String inputPath = parser.get("sourcePath");
        log.info("inputPath: {}", inputPath);

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

    private static void writeUpdates(JavaRDD<ProjectResultSet> potentialUpdates, String outputPath){
        potentialUpdates.map(u -> OBJECT_MAPPER.writeValueAsString(u))
                .saveAsTextFile(outputPath, GzipCodec.class);
    }
        //JavaPairRDD<String, TypedRow> result_result = getResultResultSemRel(allowedsemrel, relations);

//        JavaPairRDD<String, TypedRow> result_project = relations
//                .filter(r -> !r.getDataInfo().getDeletedbyinference())
//                .filter(r -> RELATION_RESULT_PROJECT_REL_CLASS.equals(r.getRelClass())
//                        && RELATION_RESULTPROJECT_REL_TYPE.equals(r.getRelType()))
//                .map(rel ->{
//
//                        TypedRow tr = new TypedRow();
//                        tr.setSourceId(rel.getSource());
//                        tr.setTargetId(rel.getTarget());
//                        return tr;
//                })
//                .mapToPair(toPair());
//
//        //relationships from project to result. One pair for each relationship for results having allowed semantics relation with another result
//        JavaPairRDD<String, TypedRow> project_result = result_project.join(result_result)
//                .map(c -> {
//                    String projectId = c._2()._1().getTargetId();
//                    String resultId = c._2()._2().getTargetId();
//                    TypedRow tr = new TypedRow(); tr.setSourceId(projectId); tr.setTargetId(resultId);
//                    return tr;
//                })
//                .mapToPair(toPair());
//
//        //relationships from project to result. One Pair for each project => project id list of results related to the project
//        JavaPairRDD<String, TypedRow> project_results = relations
//                .filter(r -> !r.getDataInfo().getDeletedbyinference())
//                .filter(r -> RELATION_PROJECT_RESULT_REL_CLASS.equals(r.getRelClass()) && RELATION_RESULTPROJECT_REL_TYPE.equals(r.getRelType()))
//                .map(r -> {
//                    TypedRow tr = new TypedRow(); tr.setSourceId(r.getSource()); tr.setTargetId(r.getTarget());
//                    return tr;
//                })
//                .mapToPair(toPair())
//                .reduceByKey((a, b) -> {
//                    if (a == null) {
//                        return b;
//                    }
//                    if (b == null) {
//                        return a;
//                    }
//                    a.addAll(b.getAccumulator());
//                    return a;
//                });
//
//
//
//        JavaRDD<Relation> newRels = project_result.join(project_results)
//                .flatMap(c -> {
//                    String resId = c._2()._1().getTargetId();
//
//                    if (c._2()._2().getAccumulator().contains(resId)) {
//                        return null;
//                    }
//                    String progId = c._2()._1().getSourceId();
//                    List<Relation> rels = new ArrayList();
//
//                    rels.add(getRelation(progId, resId, RELATION_PROJECT_RESULT_REL_CLASS,
//                            RELATION_RESULTPROJECT_REL_TYPE, RELATION_RESULTPROJECT_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
//                            PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_ID, PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_NAME));
//                    rels.add(getRelation(resId, progId, RELATION_RESULT_PROJECT_REL_CLASS,
//                            RELATION_RESULTPROJECT_REL_TYPE, RELATION_RESULTPROJECT_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
//                            PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_ID, PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_NAME));
//                    return rels.iterator();
//                })
//                .cache();
//
//        newRels.map(p -> new ObjectMapper().writeValueAsString(p))
//                .saveAsTextFile(outputPath + "/relation_new");
//
//        newRels.union(relations).map(p -> new ObjectMapper().writeValueAsString(p))
//                .saveAsTextFile(outputPath + "/relation");

    //}


    private static Dataset<ProjectResultSet> readAssocProjectResults(SparkSession spark, String potentialUpdatePath) {
        return spark
                .read()
                .textFile(potentialUpdatePath)
                .map(value -> OBJECT_MAPPER.readValue(value, ProjectResultSet.class), Encoders.bean(ProjectResultSet.class));
    }

}
