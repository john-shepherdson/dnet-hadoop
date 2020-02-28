package eu.dnetlib.dhp.projecttoresult;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.TypedRow;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.PropagationConstant.toPair;

public class SparkResultToProjectThroughSemRelJob {
    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkResultToProjectThroughSemRelJob.class.getResourceAsStream("/eu/dnetlib/dhp/projecttoresult/input_projecttoresult_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkResultToProjectThroughSemRelJob.class.getSimpleName())
                .master(parser.get("master"))
                .enableHiveSupport()
                .getOrCreate();

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("sourcePath");
        final String outputPath = "/tmp/provision/propagation/projecttoresult";

        final List<String> allowedsemrel = Arrays.asList(parser.get("allowedsemrels").split(";"));

        File directory = new File(outputPath);

        if (!directory.exists()) {
            directory.mkdirs();
        }

        JavaRDD<Relation> relations = sc.sequenceFile(inputPath + "/relation", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Relation.class)).cache();

        JavaPairRDD<String, TypedRow> result_result = getResultResultSemRel(allowedsemrel, relations);

        JavaPairRDD<String, TypedRow> result_project = relations
                .filter(r -> !r.getDataInfo().getDeletedbyinference())
                .filter(r -> RELATION_RESULT_PROJECT_REL_CLASS.equals(r.getRelClass()) && RELATION_RESULTPROJECT_REL_TYPE.equals(r.getRelType()))
                .map(r -> new TypedRow().setSourceId(r.getSource()).setTargetId(r.getTarget()))
                .mapToPair(toPair());

        //relationships from project to result. One pair for each relationship for results having allowed semantics relation with another result
        JavaPairRDD<String, TypedRow> project_result = result_project.join(result_result)
                .map(c -> {
                    String projectId = c._2()._1().getTargetId();
                    String resultId = c._2()._2().getTargetId();
                    return new TypedRow().setSourceId(projectId).setTargetId(resultId);
                })
                .mapToPair(toPair());

        //relationships from project to result. One Pair for each project => project id list of results related to the project
        JavaPairRDD<String, TypedRow> project_results = relations
                .filter(r -> !r.getDataInfo().getDeletedbyinference())
                .filter(r -> RELATION_PROJECT_RESULT_REL_CLASS.equals(r.getRelClass()) && RELATION_RESULTPROJECT_REL_TYPE.equals(r.getRelType()))
                .map(r -> new TypedRow().setSourceId(r.getSource()).setTargetId(r.getTarget()))
                .mapToPair(toPair())
                .reduceByKey((a, b) -> {
                    if (a == null) {
                        return b;
                    }
                    if (b == null) {
                        return a;
                    }
                    a.addAll(b.getAccumulator());
                    return a;
                });



        JavaRDD<Relation> newRels = project_result.join(project_results)
                .flatMap(c -> {
                    String resId = c._2()._1().getTargetId();

                    if (c._2()._2().getAccumulator().contains(resId)) {
                        return null;
                    }
                    String progId = c._2()._1().getSourceId();
                    List<Relation> rels = new ArrayList();

                    rels.add(getRelation(progId, resId, RELATION_PROJECT_RESULT_REL_CLASS,
                            RELATION_RESULTPROJECT_REL_TYPE, RELATION_RESULTPROJECT_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
                            PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_ID, PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_NAME));
                    rels.add(getRelation(resId, progId, RELATION_RESULT_PROJECT_REL_CLASS,
                            RELATION_RESULTPROJECT_REL_TYPE, RELATION_RESULTPROJECT_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
                            PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_ID, PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_NAME));
                    return rels.iterator();
                })
                .cache();

        newRels.map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath + "/relation_new");

        newRels.union(relations).map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath + "/relation");

    }




}
