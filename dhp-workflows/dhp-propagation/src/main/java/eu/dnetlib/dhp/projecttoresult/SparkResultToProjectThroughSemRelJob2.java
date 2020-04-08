package eu.dnetlib.dhp.projecttoresult;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.TypedRow;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.util.*;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.PropagationConstant.toPair;

public class SparkResultToProjectThroughSemRelJob {
    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkResultToProjectThroughSemRelJob.class.getResourceAsStream("/eu/dnetlib/dhp/projecttoresult/input_projecttoresult_parameters.json")));
        parser.parseArgument(args);
        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkResultToProjectThroughSemRelJob.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("sourcePath");
        final String outputPath = "/tmp/provision/propagation/projecttoresult";

        final List<String> allowedsemrel = Arrays.asList(parser.get("allowedsemrels").split(";"));

        createOutputDirs(outputPath, FileSystem.get(spark.sparkContext().hadoopConfiguration()));

        JavaRDD<Relation> all_relations = sc.textFile(inputPath + "/relation")
                .map(item -> new ObjectMapper().readValue(item, Relation.class));

        JavaRDD<Relation> relations = all_relations.filter(r -> !r.getDataInfo().getDeletedbyinference()).cache();

        JavaRDD<Relation> result_result = relations
                .filter(r -> allowedsemrel.contains(r.getRelClass()) && RELATION_RESULTRESULT_REL_TYPE.equals(r.getRelType()));

        org.apache.spark.sql.Dataset<Relation> resres_relation = spark.createDataset(result_result.rdd(),
                Encoders.bean(Relation.class));

        JavaRDD<Relation> result_project = relations
                .filter(r -> RELATION_RESULT_PROJECT_REL_CLASS.equals(r.getRelClass())
                        && RELATION_RESULTPROJECT_REL_TYPE.equals(r.getRelType()));

        org.apache.spark.sql.Dataset<Relation> resproj_relation = spark.createDataset(result_project.rdd(),
                Encoders.bean(Relation.class));

        resres_relation.createOrReplaceTempView("resres_relation");
        resproj_relation.createOrReplaceTempView("resproj_relation");

        String query ="SELECT proj, collect_set(r1target) result_set " +
                "FROM (" +
                "      SELECT r1.source as sourcer, r1.relclass as r1rel, r1.target as r1target, r2.target as proj " +
                "      FROM resres_relation r1 " +
                "      JOIN resproj_relation r2 " +
                "      ON r1.source = r2.source " +
                "      ) tmp " +
                "GROUP BY proj ";

        Dataset<Row> toaddrelations  = spark.sql(query);


        JavaPairRDD<String, TypedRow> project_resultlist = relations
                .filter(r -> RELATION_PROJECT_RESULT_REL_CLASS.equals(r.getRelClass()))
                .map(r -> {
                    TypedRow tp = new TypedRow();
                    tp.setSourceId(r.getSource());
                    tp.add(r.getTarget());
                    return tp;
                }).mapToPair(toPair())
                .reduceByKey((a, b) -> {
                    if (a == null) {
                        return b;
                    }
                    if (b == null) {
                        return a;
                    }

                    a.addAll(b.getAccumulator());
                    return a;
                }).cache();


        JavaRDD<Relation> new_relations = toaddrelations.toJavaRDD().mapToPair(r -> new Tuple2<>(r.getString(0), r.getList(1)))
                .leftOuterJoin(project_resultlist)
                .flatMap(c -> {
                    List<Object> toAddRel = new ArrayList<>();
                    toAddRel.addAll(c._2()._1());
                    if (c._2()._2().isPresent()) {
                        Set<String> originalRels = c._2()._2().get().getAccumulator();
                        for (String o : originalRels) {
                            if (toAddRel.contains(o)) {
                                toAddRel.remove(o);
                            }
                        }
                    }
                    List<Relation> relationList = new ArrayList<>();
                    String projId = c._1();
                    for (Object r : toAddRel) {
                        String rId = (String) r;
                        relationList.add(getRelation(rId, projId, RELATION_RESULT_PROJECT_REL_CLASS, RELATION_RESULTPROJECT_REL_TYPE,
                                RELATION_RESULTPROJECT_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
                                PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_ID,
                                PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_NAME));
                        relationList.add(getRelation(projId, rId, RELATION_PROJECT_RESULT_REL_CLASS, RELATION_RESULTPROJECT_REL_TYPE,
                                RELATION_RESULTPROJECT_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
                                PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_ID,
                                PROPAGATION_RELATION_RESULT_PROJECT_SEM_REL_CLASS_NAME));

                    }
                    return relationList.iterator();
                }).cache();

        toaddrelations.toJavaRDD().map(r->new ObjectMapper().writeValueAsString(r))
                .saveAsTextFile(outputPath + "/toupdaterelations");

        new_relations.map(r-> new ObjectMapper().writeValueAsString(r))
                .saveAsTextFile(outputPath + "/new_relations" );

        all_relations.union(new_relations)
                .map(r -> new ObjectMapper().writeValueAsString(r))
                .saveAsTextFile(outputPath + "/relation");


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

    }




}
