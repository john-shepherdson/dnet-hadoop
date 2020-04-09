package eu.dnetlib.dhp.projecttoresult;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.Relation;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static eu.dnetlib.dhp.PropagationConstant.*;

public class SparkResultToProjectThroughSemRelJob3 {
    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(SparkResultToProjectThroughSemRelJob3.class
                        .getResourceAsStream("/eu/dnetlib/dhp/projecttoresult/input_projecttoresult_parameters.json")));
        parser.parseArgument(args);


        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkResultToProjectThroughSemRelJob3.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();


        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("sourcePath");
        final String outputPath = "/tmp/provision/propagation/projecttoresult";
        boolean writeUpdates = "true".equals(parser.get("writeUpdate"));
        boolean saveGraph = "true".equals(parser.get("saveGraph"));

        final List<String> allowedsemrel = Arrays.asList(parser.get("allowedsemrels").split(";"));

        createOutputDirs(outputPath, FileSystem.get(spark.sparkContext().hadoopConfiguration()));

        Dataset<Relation> relation = spark.createDataset(sc.textFile(inputPath + "/relation")
                .map(item -> new ObjectMapper().readValue(item, Relation.class)).rdd(), Encoders.bean(Relation.class));
        
        relation.createOrReplaceTempView("relation");

        String query = "Select source, target " +
                "from relation " +
                "where datainfo.deletedbyinference = false and relClass = '" + RELATION_RESULT_PROJECT_REL_CLASS + "'";

        Dataset<Row> resproj_relation = spark.sql(query);
        
        query = "Select source, target " +
                "from relation " +
                "where datainfo.deletedbyinference = false  " + getConstraintList(" relClass = '", allowedsemrel );

        Dataset<Row> resres_relation = spark.sql(query);
        resres_relation.createOrReplaceTempView("resres_relation");
        resproj_relation.createOrReplaceTempView("resproj_relation");

        query ="SELECT proj, collect_set(r1target) result_set " +
                "FROM (" +
                "      SELECT r1.source as source, r1.target as r1target, r2.target as proj " +
                "      FROM resres_relation r1 " +
                "      JOIN resproj_relation r2 " +
                "      ON r1.source = r2.source " +
                "      ) tmp " +
                "GROUP BY proj ";

        Dataset<Row> toaddrelations  = spark.sql(query);
        
        query = "select target, collect_set(source) result_list from " +
                "resproj_relation " +
                "group by target";

        Dataset<Row> project_resultlist = spark.sql(query);

        //if (writeUpdaes){
            toaddrelations.toJavaRDD().map(r->new ObjectMapper().writeValueAsString(r))
                    .saveAsTextFile(outputPath + "/toupdaterelations");
        //}

        if(saveGraph){
            JavaRDD<Relation> new_relations = toaddrelations.toJavaRDD().mapToPair(r -> new Tuple2<>(r.getString(0), r.getList(1)))
                    .leftOuterJoin(project_resultlist.toJavaRDD().mapToPair(pr -> new Tuple2<>(pr.getString(0), pr.getList(1))))
                    .flatMap(c -> {
                        List<Object> toAddRel = new ArrayList<>();
                        toAddRel.addAll(c._2()._1());
                        if (c._2()._2().isPresent()) {
                            List<Object> originalRels = c._2()._2().get();
                            for (Object o : originalRels) {
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
                        if(relationList.size()==0){
                            return null;
                        }
                        return relationList.iterator();
                    }).filter(r -> !(r==null));


            new_relations.map(r-> new ObjectMapper().writeValueAsString(r))
                    .saveAsTextFile(outputPath + "/new_relations" );

            sc.textFile(inputPath + "/relation")
                    .map(item -> new ObjectMapper().readValue(item, Relation.class))
                    .union(new_relations)
                    .map(r -> new ObjectMapper().writeValueAsString(r))
                    .saveAsTextFile(outputPath + "/relation");

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

    }




}
