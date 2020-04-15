package eu.dnetlib.dhp.resulttoorganizationfrominstrepo;

import eu.dnetlib.dhp.TypedRow;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.io.Text;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.*;

import eu.dnetlib.dhp.schema.oaf.*;
import scala.Tuple2;

import static eu.dnetlib.dhp.PropagationConstant.*;

public class SparkResultToOrganizationFromIstRepoJob {
    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkResultToOrganizationFromIstRepoJob.class.getResourceAsStream("/eu/dnetlib/dhp/resulttoorganizationfrominstrepo/input_propagationresulaffiliationfrominstrepo_parameters.json")));
        parser.parseArgument(args);
        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkResultToOrganizationFromIstRepoJob.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("sourcePath");
        final String outputPath = "/tmp/provision/propagation/resulttoorganizationfrominstitutionalrepositories";

        createOutputDirs(outputPath, FileSystem.get(spark.sparkContext().hadoopConfiguration()));

        org.apache.spark.sql.Dataset<Datasource> datasource = spark.createDataset(sc.textFile(inputPath + "/datasource")
                .map(item -> new ObjectMapper().readValue(item, Datasource.class)).rdd(), Encoders.bean(Datasource.class));

        org.apache.spark.sql.Dataset<Relation> relation = spark.createDataset(sc.textFile(inputPath + "/relation")
                .map(item -> new ObjectMapper().readValue(item, Relation.class)).rdd(), Encoders.bean(Relation.class));

        org.apache.spark.sql.Dataset<Organization> organization = spark.createDataset(sc.textFile(inputPath + "/organization")
                .map(item -> new ObjectMapper().readValue(item, Organization.class)).rdd(), Encoders.bean(Organization.class));

        datasource.createOrReplaceTempView("datasource");
        relation.createOrReplaceTempView("relation");
        organization.createOrReplaceTempView("organization");

        String query = "SELECT source ds, target org " +
                "FROM ( SELECT id " +
                "FROM datasource " +
                "WHERE datasourcetype.classid = 'pubsrepository::institutional' " +
                "AND datainfo.deletedbyinference = false  ) d " +
                "JOIN ( SELECT source, target " +
                "FROM relation " +
                "WHERE relclass = 'provides' " +
                "AND datainfo.deletedbyinference = false ) rel " +
                "ON d.id = rel.source ";

        org.apache.spark.sql.Dataset<Row> rels = spark.sql(query);
        rels.createOrReplaceTempView("rels");

        org.apache.spark.sql.Dataset<Dataset> dataset = spark.createDataset(sc.textFile(inputPath + "/dataset")
                        .map(item -> new ObjectMapper().readValue(item, eu.dnetlib.dhp.schema.oaf.Dataset.class)).rdd(),
                Encoders.bean(eu.dnetlib.dhp.schema.oaf.Dataset.class));

        org.apache.spark.sql.Dataset<OtherResearchProduct> other = spark.createDataset(sc.textFile(inputPath + "/otherresearchproduct")
                        .map(item -> new ObjectMapper().readValue(item, eu.dnetlib.dhp.schema.oaf.OtherResearchProduct.class)).rdd(),
                Encoders.bean(eu.dnetlib.dhp.schema.oaf.OtherResearchProduct.class));

        org.apache.spark.sql.Dataset<Software> software = spark.createDataset(sc.textFile(inputPath + "/software")
                        .map(item -> new ObjectMapper().readValue(item, eu.dnetlib.dhp.schema.oaf.Software.class)).rdd(),
                Encoders.bean(eu.dnetlib.dhp.schema.oaf.Software.class));

        org.apache.spark.sql.Dataset<Publication> publication = spark.createDataset(sc.textFile(inputPath + "/publication")
                        .map(item -> new ObjectMapper().readValue(item, eu.dnetlib.dhp.schema.oaf.Publication.class)).rdd(),
                Encoders.bean(eu.dnetlib.dhp.schema.oaf.Publication.class));


        software.createOrReplaceTempView("software");
        final JavaRDD<Row> toupdateresultsoftware = propagateOnResult(spark, "software");

        dataset.createOrReplaceTempView("dataset");
        final JavaRDD<Row> toupdateresultdataset = propagateOnResult(spark, "dataset");

        other.createOrReplaceTempView("other");
        final JavaRDD<Row> toupdateresultother = propagateOnResult(spark, "other");

        publication.createOrReplaceTempView("publication");
        final JavaRDD<Row> toupdateresultpublication = propagateOnResult(spark, "publication");

        writeUpdates(toupdateresultsoftware, toupdateresultdataset, toupdateresultother, toupdateresultpublication, outputPath);


        query = "Select source resultId, collect_set(target) org_list " +
                "from relation " +
                "where datainfo.deletedbyinference = false " +
                "and relClass = '" + RELATION_RESULT_ORGANIZATION_REL_CLASS +"' " +
                "group by source";

        JavaRDD<Row> result_orglist = spark.sql(query).toJavaRDD();

        JavaPairRDD<String, List<Object>> toupdateunion = toupdateresultdataset.mapToPair(d -> new Tuple2<>(d.getString(0), d.getList(1)))
                .union(toupdateresultother.mapToPair(o -> new Tuple2<>(o.getString(0), o.getList(1))))
                .union(toupdateresultpublication.mapToPair(p -> new Tuple2<>(p.getString(0), p.getList(1))))
                .union(toupdateresultsoftware.mapToPair(s -> new Tuple2<>(s.getString(0), s.getList(1))));

        JavaRDD<Relation> new_rels = getNewRels(result_orglist.mapToPair(r -> new Tuple2<>(r.getString(0), r.getList(1))),
                toupdateunion);



        sc.textFile(inputPath + "/relation")
                .map(item -> new ObjectMapper().readValue(item, Relation.class))
                .union(new_rels)
                .map(r -> new ObjectMapper().writeValueAsString(r))
                .saveAsTextFile(outputPath + "/relation");


    }

        private static JavaRDD<Relation> getNewRels(JavaPairRDD<String, List<String>> relationOrgs, JavaPairRDD <String, List<Object>> newRels){
        return newRels
                .leftOuterJoin(relationOrgs)
                .flatMap(c -> {
                    List<Object> toAddOrgs = new ArrayList<>();
                    toAddOrgs.addAll(c._2()._1());
                    if (c._2()._2().isPresent()) {
                        Set<String> originalOrgs = new HashSet<>();
                        originalOrgs.addAll(c._2()._2().get());
                        for (Object oId : originalOrgs) {
                            if (toAddOrgs.contains(oId)) {
                                toAddOrgs.remove(oId);
                            }
                        }
                    }
                    List<Relation> relationList = new ArrayList<>();
                    String resId = c._1();
                    for (Object org : toAddOrgs) {
                        relationList.add(getRelation((String)org, resId, RELATION_ORGANIZATION_RESULT_REL_CLASS,
                                RELATION_RESULTORGANIZATION_REL_TYPE, RELATION_RESULTORGANIZATION_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
                                PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID, PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME));
                        relationList.add(getRelation(resId, (String)org, RELATION_RESULT_ORGANIZATION_REL_CLASS,
                                RELATION_RESULTORGANIZATION_REL_TYPE, RELATION_RESULTORGANIZATION_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
                                PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID, PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME));

                    }
                    return relationList.iterator();
                });

    }


    private static void writeUpdates(JavaRDD<Row> toupdateresultsoftware, JavaRDD<Row> toupdateresultdataset, JavaRDD<Row> toupdateresultother, JavaRDD<Row> toupdateresultpublication, String outputPath) {
        createUpdateForRelationWrite(toupdateresultsoftware, outputPath, "update_software");
        createUpdateForRelationWrite(toupdateresultdataset, outputPath, "update_dataset");
        createUpdateForRelationWrite(toupdateresultother, outputPath, "update_other");
        createUpdateForRelationWrite(toupdateresultpublication, outputPath, "update_publication");

    }

    private static void createUpdateForRelationWrite(JavaRDD<Row> toupdaterelation, String outputPath, String update_type) {
        toupdaterelation.flatMap(s -> {
            List<Relation> relationList = new ArrayList<>();
            List<String> orgs = s.getList(1);
            String resId = s.getString(0);
            for (String org : orgs) {
                relationList.add(getRelation(org, resId, RELATION_ORGANIZATION_RESULT_REL_CLASS,
                        RELATION_RESULTORGANIZATION_REL_TYPE, RELATION_RESULTORGANIZATION_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
                        PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID, PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME));
                relationList.add(getRelation(resId, org, RELATION_RESULT_ORGANIZATION_REL_CLASS,
                        RELATION_RESULTORGANIZATION_REL_TYPE, RELATION_RESULTORGANIZATION_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
                        PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID, PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME));

            }
            return relationList.iterator();
        }).map(s -> new ObjectMapper().writeValueAsString(s)).saveAsTextFile(outputPath + "/" + update_type);
    }

    private static JavaRDD<Row> propagateOnResult(SparkSession spark, String table) {
        String query;
        query = "SELECT id, inst.collectedfrom.key cf , inst.hostedby.key hb " +
                "FROM ( SELECT id, instance " +
                "FROM " + table +
                " WHERE datainfo.deletedbyinference = false)  ds " +
                "LATERAL VIEW EXPLODE(instance) i AS inst";
        org.apache.spark.sql.Dataset<Row> cfhb = spark.sql(query);
        cfhb.createOrReplaceTempView("cfhb");

        return organizationPropagationAssoc(spark, "cfhb").toJavaRDD();

    }

    private static org.apache.spark.sql.Dataset<Row> organizationPropagationAssoc(SparkSession spark, String cfhbTable){
        String  query = "SELECT id, collect_set(org) org "+
                "FROM ( SELECT id, org " +
                "FROM rels " +
                "JOIN " + cfhbTable  +
                " ON cf = ds     " +
                "UNION ALL " +
                "SELECT id , org     " +
                "FROM rels " +
                "JOIN " + cfhbTable  +
                " ON hb = ds ) tmp " +
                "GROUP BY id";
        return spark.sql(query);
    }

}

//package eu.dnetlib.dhp.resulttoorganizationfrominstrepo;
//
//import eu.dnetlib.dhp.TypedRow;
//import eu.dnetlib.dhp.application.ArgumentApplicationParser;
//import org.apache.commons.io.IOUtils;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.Encoders;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.apache.hadoop.io.Text;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import java.io.File;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Set;
//
//import eu.dnetlib.dhp.schema.oaf.*;
//import scala.Tuple2;
//
//import static eu.dnetlib.dhp.PropagationConstant.*;
//
//public class SparkResultToOrganizationFromIstRepoJob {
//    public static void main(String[] args) throws Exception {
//
//        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkResultToOrganizationFromIstRepoJob.class.getResourceAsStream("/eu/dnetlib/dhp/resulttoorganizationfrominstrepo/input_propagationresulaffiliationfrominstrepo_parameters.json")));
//        parser.parseArgument(args);
//        SparkConf conf = new SparkConf();
//        conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));
//        final SparkSession spark = SparkSession
//                .builder()
//                .appName(SparkResultToOrganizationFromIstRepoJob.class.getSimpleName())
//                .master(parser.get("master"))
//                .config(conf)
//                .enableHiveSupport()
//                .getOrCreate();
//
//        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
//        final String inputPath = parser.get("sourcePath");
//        final String outputPath = "/tmp/provision/propagation/resulttoorganizationfrominstitutionalrepositories";
//
//        createOutputDirs(outputPath, FileSystem.get(spark.sparkContext().hadoopConfiguration()));
//
//        org.apache.spark.sql.Dataset<Datasource> datasource = spark.createDataset(sc.textFile(inputPath + "/datasource")
//                .map(item -> new ObjectMapper().readValue(item, Datasource.class))
//                .rdd(), Encoders.bean(Datasource.class));
//
//        JavaRDD<Relation> relation_rdd_all = sc.textFile(inputPath + "/relation")
//                .map(item -> new ObjectMapper().readValue(item, Relation.class));
//        JavaRDD<Relation> relation_rdd = relation_rdd_all.filter(r -> !r.getDataInfo().getDeletedbyinference()).cache();
//
//        org.apache.spark.sql.Dataset<Relation> relation = spark.createDataset(relation_rdd.rdd(), Encoders.bean(Relation.class));
//
//        org.apache.spark.sql.Dataset<Organization> organization = spark.createDataset(sc.textFile(inputPath + "/organziation")
//                .map(item -> new ObjectMapper().readValue(item, Organization.class)).rdd(), Encoders.bean(Organization.class));
//
//        datasource.createOrReplaceTempView("datasource");
//        relation.createOrReplaceTempView("relation");
//        organization.createOrReplaceTempView("organization");
//
//        String query = "SELECT source ds, target org " +
//                "FROM ( SELECT id " +
//                "FROM datasource " +
//                "WHERE datasourcetype.classid = 'pubsrepository::institutional' " +
//                "AND datainfo.deletedbyinference = false  ) d " +
//                "JOIN ( SELECT source, target " +
//                "FROM relation " +
//                "WHERE relclass = 'provides' " +
//                "AND datainfo.deletedbyinference = false ) rel " +
//                "ON d.id = rel.source ";
//
//        org.apache.spark.sql.Dataset<Row> rels = spark.sql(query);
//        rels.createOrReplaceTempView("rels");
//
//        org.apache.spark.sql.Dataset<Dataset> dataset = spark.createDataset(sc.textFile(inputPath + "/dataset")
//                        .map(item -> new ObjectMapper().readValue(item, eu.dnetlib.dhp.schema.oaf.Dataset.class)).rdd(),
//                Encoders.bean(eu.dnetlib.dhp.schema.oaf.Dataset.class));
//
//        org.apache.spark.sql.Dataset<OtherResearchProduct> other = spark.createDataset(sc.textFile(inputPath + "/otherresearchproduct")
//                        .map(item -> new ObjectMapper().readValue(item, eu.dnetlib.dhp.schema.oaf.OtherResearchProduct.class)).rdd(),
//                Encoders.bean(eu.dnetlib.dhp.schema.oaf.OtherResearchProduct.class));
//
//        org.apache.spark.sql.Dataset<Software> software = spark.createDataset(sc.textFile(inputPath + "/software")
//                        .map(item -> new ObjectMapper().readValue(item, eu.dnetlib.dhp.schema.oaf.Software.class)).rdd(),
//                Encoders.bean(eu.dnetlib.dhp.schema.oaf.Software.class));
//
//        org.apache.spark.sql.Dataset<Publication> publication = spark.createDataset(sc.textFile(inputPath + "/publication")
//                        .map(item -> new ObjectMapper().readValue(item, eu.dnetlib.dhp.schema.oaf.Publication.class)).rdd(),
//                Encoders.bean(eu.dnetlib.dhp.schema.oaf.Publication.class));
//
//
//        software.createOrReplaceTempView("software");
//        final JavaRDD<Row> toupdateresultsoftware = propagateOnResult(spark, "software");
//
//        dataset.createOrReplaceTempView("dataset");
//        final JavaRDD<Row> toupdateresultdataset = propagateOnResult(spark, "dataset");
//
//        other.createOrReplaceTempView("other");
//        final JavaRDD<Row> toupdateresultother = propagateOnResult(spark, "other");
//
//        publication.createOrReplaceTempView("publication");
//        final JavaRDD<Row> toupdateresultpublication = propagateOnResult(spark, "publication");
//
//        writeUpdates(toupdateresultsoftware, toupdateresultdataset, toupdateresultother, toupdateresultpublication, outputPath);
//
//        JavaPairRDD<String, TypedRow> relation_rdd_pair = relation_rdd
//                .filter(r -> RELATION_RESULT_ORGANIZATION_REL_CLASS.equals(r.getRelClass()))
//                .map(r -> {
//                    TypedRow tp = new TypedRow();
//                    tp.setSourceId(r.getSource());
//                    tp.add(r.getTarget());
//                    return tp;
//                }).mapToPair(toPair())
//                .reduceByKey((a, b) -> {
//                    if (a == null) {
//                        return b;
//                    }
//                    if (b == null) {
//                        return a;
//                    }
//
//                    a.addAll(b.getAccumulator());
//                    return a;
//                }).cache();
//
//
//        JavaRDD<Relation> new_rels = getNewRels(relation_rdd_pair,
//                toupdateresultother.mapToPair(s -> new Tuple2<>(s.getString(0), s.getList(1))))
//                .union(getNewRels(relation_rdd_pair,
//                        toupdateresultsoftware.mapToPair(s -> new Tuple2<>(s.getString(0), s.getList(1)))))
//                .union(getNewRels(relation_rdd_pair,
//                        toupdateresultdataset.mapToPair(s -> new Tuple2<>(s.getString(0), s.getList(1)))))
//                .union(getNewRels(relation_rdd_pair,
//                        toupdateresultpublication.mapToPair(s -> new Tuple2<>(s.getString(0), s.getList(1)))));
//
//
//        relation_rdd_all.union(new_rels).map(r -> new ObjectMapper().writeValueAsString(r))
//                .saveAsTextFile(outputPath + "/relation");
//
//    }
//
//    private static JavaRDD<Relation> getNewRels(JavaPairRDD<String, TypedRow> relation_rdd_pair, JavaPairRDD <String, List<String>> newRels){
//        return newRels//.mapToPair(s -> new Tuple2<>(s.getString(0), s.getList(1)))
//                .leftOuterJoin(relation_rdd_pair)
//                .flatMap(c -> {
//                    List<String> toAddOrgs = c._2()._1();
//                    if (c._2()._2().isPresent()) {
//                        Set<String> originalOrgs = c._2()._2().get().getAccumulator();
//                        for (String oId : toAddOrgs) {
//                            if (originalOrgs.contains(oId)) {
//                                toAddOrgs.remove(oId);
//                            }
//                        }
//                    }
//                    List<Relation> relationList = new ArrayList<>();
//                    String resId = c._1();
//                    for (String org : toAddOrgs) {
//                        relationList.add(getRelation(org, resId, RELATION_ORGANIZATION_RESULT_REL_CLASS,
//                                RELATION_RESULTORGANIZATION_REL_TYPE, RELATION_RESULTORGANIZATION_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
//                                PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID, PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME));
//                        relationList.add(getRelation(resId, org, RELATION_RESULT_ORGANIZATION_REL_CLASS,
//                                RELATION_RESULTORGANIZATION_REL_TYPE, RELATION_RESULTORGANIZATION_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
//                                PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID, PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME));
//
//                    }
//                    return relationList.iterator();
//                });
//
//    }
//
//    private static void writeUpdates(JavaRDD<Row> toupdateresultsoftware, JavaRDD<Row> toupdateresultdataset, JavaRDD<Row> toupdateresultother, JavaRDD<Row> toupdateresultpublication, String outputPath) {
//        createUpdateForRelationWrite(toupdateresultsoftware, outputPath, "update_software");
//        createUpdateForRelationWrite(toupdateresultdataset, outputPath, "update_dataset");
//        createUpdateForRelationWrite(toupdateresultother, outputPath, "update_other");
//        createUpdateForRelationWrite(toupdateresultpublication, outputPath, "update_publication");
//
//    }
//
//    private static void createUpdateForRelationWrite(JavaRDD<Row> toupdaterelation, String outputPath, String update_type) {
//        toupdaterelation.flatMap(s -> {
//            List<Relation> relationList = new ArrayList<>();
//            List<String> orgs = s.getList(1);
//            String resId = s.getString(0);
//            for (String org : orgs) {
//                relationList.add(getRelation(org, resId, RELATION_ORGANIZATION_RESULT_REL_CLASS,
//                        RELATION_RESULTORGANIZATION_REL_TYPE, RELATION_RESULTORGANIZATION_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
//                        PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID, PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME));
//                relationList.add(getRelation(resId, org, RELATION_RESULT_ORGANIZATION_REL_CLASS,
//                        RELATION_RESULTORGANIZATION_REL_TYPE, RELATION_RESULTORGANIZATION_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
//                        PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID, PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME));
//
//            }
//            return relationList.iterator();
//        }).map(s -> new ObjectMapper().writeValueAsString(s)).saveAsTextFile(outputPath + "/" + update_type);
//    }
//
//    private static JavaRDD<Row> propagateOnResult(SparkSession spark, String table) {
//        String query;
//        query = "SELECT id, inst.collectedfrom.key cf , inst.hostedby.key hb " +
//                "FROM ( SELECT id, instance " +
//                "FROM " + table +
//                " WHERE datainfo.deletedbyinference = false)  ds " +
//                "LATERAL VIEW EXPLODE(instance) i AS inst";
//        org.apache.spark.sql.Dataset<Row> cfhb = spark.sql(query);
//        cfhb.createOrReplaceTempView("cfhb");
//
//        return organizationPropagationAssoc(spark, "cfhb").toJavaRDD();
//
//    }
//
//    private static org.apache.spark.sql.Dataset<Row> organizationPropagationAssoc(SparkSession spark, String cfhbTable){
//        String  query = "SELECT id, collect_set(org) org "+
//                "FROM ( SELECT id, org " +
//                "FROM rels " +
//                "JOIN " + cfhbTable  +
//                " ON cf = ds     " +
//                "UNION ALL " +
//                "SELECT id , org     " +
//                "FROM rels " +
//                "JOIN " + cfhbTable  +
//                " ON hb = ds ) tmp " +
//                "GROUP BY id";
//        return spark.sql(query);
//    }
//
//}
