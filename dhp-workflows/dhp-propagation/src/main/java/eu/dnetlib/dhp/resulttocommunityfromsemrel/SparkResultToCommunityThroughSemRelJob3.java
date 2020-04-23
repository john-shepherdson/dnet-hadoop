package eu.dnetlib.dhp.resulttocommunityfromsemrel;

import static eu.dnetlib.dhp.PropagationConstant.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.QueryInformationSystem;
import eu.dnetlib.dhp.TypedRow;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class SparkResultToCommunityThroughSemRelJob3 {
    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser =
                new ArgumentApplicationParser(
                        IOUtils.toString(
                                SparkResultToCommunityThroughSemRelJob3.class.getResourceAsStream(
                                        "/eu/dnetlib/dhp/resulttocommunityfromsemrel/input_communitytoresult_parameters.json")));
        parser.parseArgument(args);

        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));
        final SparkSession spark =
                SparkSession.builder()
                        .appName(SparkResultToCommunityThroughSemRelJob3.class.getSimpleName())
                        .master(parser.get("master"))
                        .config(conf)
                        .enableHiveSupport()
                        .getOrCreate();

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("sourcePath");
        final String outputPath = "/tmp/provision/propagation/resulttocommunityfromsemrel";

        final List<String> allowedsemrel = Arrays.asList(parser.get("allowedsemrels").split(";"));

        final List<String> communityIdList =
                QueryInformationSystem.getCommunityList(parser.get("isLookupUrl"));

        createOutputDirs(outputPath, FileSystem.get(spark.sparkContext().hadoopConfiguration()));

        JavaRDD<Publication> publication_rdd =
                sc.textFile(inputPath + "/publication")
                        .map(item -> new ObjectMapper().readValue(item, Publication.class));

        JavaRDD<Dataset> dataset_rdd =
                sc.textFile(inputPath + "/dataset")
                        .map(item -> new ObjectMapper().readValue(item, Dataset.class));

        JavaRDD<OtherResearchProduct> orp_rdd =
                sc.textFile(inputPath + "/otherresearchproduct")
                        .map(
                                item ->
                                        new ObjectMapper()
                                                .readValue(item, OtherResearchProduct.class));

        JavaRDD<Software> software_rdd =
                sc.textFile(inputPath + "/software")
                        .map(item -> new ObjectMapper().readValue(item, Software.class));

        JavaRDD<Relation> relation_rdd =
                sc.textFile(inputPath + "/relation")
                        .map(item -> new ObjectMapper().readValue(item, Relation.class));

        org.apache.spark.sql.Dataset<Publication> publication =
                spark.createDataset(publication_rdd.rdd(), Encoders.bean(Publication.class));

        org.apache.spark.sql.Dataset<Relation> relation =
                spark.createDataset(relation_rdd.rdd(), Encoders.bean(Relation.class));

        org.apache.spark.sql.Dataset<Dataset> dataset =
                spark.createDataset(dataset_rdd.rdd(), Encoders.bean(Dataset.class));

        org.apache.spark.sql.Dataset<OtherResearchProduct> other =
                spark.createDataset(orp_rdd.rdd(), Encoders.bean(OtherResearchProduct.class));

        org.apache.spark.sql.Dataset<Software> software =
                spark.createDataset(software_rdd.rdd(), Encoders.bean(Software.class));

        publication.createOrReplaceTempView("publication");
        relation.createOrReplaceTempView("relation");
        dataset.createOrReplaceTempView("dataset");
        software.createOrReplaceTempView("software");
        other.createOrReplaceTempView("other");

        String communitylist = getConstraintList(" co.id = '", communityIdList);

        String semrellist = getConstraintList(" relClass = '", allowedsemrel);

        String query =
                "Select source, community_context, target "
                        + "from (select id, collect_set(co.id) community_context "
                        + "from  publication "
                        + "lateral view explode (context) c as co "
                        + "where datainfo.deletedbyinference = false "
                        + communitylist
                        + " group by id) p "
                        + "JOIN "
                        + "(select * "
                        + "from relation "
                        + "where datainfo.deletedbyinference = false "
                        + semrellist
                        + ") r "
                        + "ON p.id = r.source";

        org.apache.spark.sql.Dataset<Row> publication_context = spark.sql(query);
        publication_context.createOrReplaceTempView("publication_context");

        // ( source, (mes, dh-ch-, ni), target )
        query =
                "select target , collect_set(co) "
                        + "from (select target, community_context "
                        + "from publication_context pc join publication p on "
                        + "p.id = pc.source) tmp "
                        + "lateral view explode (community_context) c as co "
                        + "group by target";

        org.apache.spark.sql.Dataset<Row> toupdatepublicationreresult = spark.sql(query);
        org.apache.spark.sql.Dataset<Row> toupdatesoftwareresult =
                getUpdateCommunitiesForTable(spark, "software");
        org.apache.spark.sql.Dataset<Row> toupdatedatasetresult =
                getUpdateCommunitiesForTable(spark, "dataset");
        org.apache.spark.sql.Dataset<Row> toupdateotherresult =
                getUpdateCommunitiesForTable(spark, "other");

        createUpdateForResultDatasetWrite(
                toupdatesoftwareresult.toJavaRDD(),
                outputPath,
                "software_update",
                PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_ID,
                PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_NAME,
                communityIdList);

        createUpdateForResultDatasetWrite(
                toupdatedatasetresult.toJavaRDD(),
                outputPath,
                "dataset_update",
                PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_ID,
                PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_NAME,
                communityIdList);

        createUpdateForResultDatasetWrite(
                toupdatepublicationreresult.toJavaRDD(),
                outputPath,
                "publication_update",
                PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_ID,
                PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_NAME,
                communityIdList);

        createUpdateForResultDatasetWrite(
                toupdateotherresult.toJavaRDD(),
                outputPath,
                "other_update",
                PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_ID,
                PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_NAME,
                communityIdList);

        updateForDatasetDataset(
                toupdatedatasetresult.toJavaRDD(),
                dataset.toJavaRDD(),
                outputPath,
                "dataset",
                PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_ID,
                PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_NAME,
                communityIdList);

        updateForOtherDataset(
                toupdateotherresult.toJavaRDD(),
                other.toJavaRDD(),
                outputPath,
                "otherresearchproduct",
                PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_ID,
                PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_NAME,
                communityIdList);

        updateForSoftwareDataset(
                toupdatesoftwareresult.toJavaRDD(),
                software.toJavaRDD(),
                outputPath,
                "software",
                PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_ID,
                PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_NAME,
                communityIdList);

        updateForPublicationDataset(
                toupdatepublicationreresult.toJavaRDD(),
                publication.toJavaRDD(),
                outputPath,
                "publication",
                PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_ID,
                PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_NAME,
                communityIdList);
        //

        /*
                JavaPairRDD<String, TypedRow> resultLinkedToCommunities = publication
                        .map(p -> getTypedRow(communityIdList, p.getContext(), p.getId(),"publication"))
                        .filter(p -> !(p == null))
                        .mapToPair(toPair())
                        .union(datasets
                                .map(p -> getTypedRow(communityIdList, p.getContext(), p.getId(),"dataset"))
                                .filter(p -> !(p == null))
                                .mapToPair(toPair())
                        )
                        .union(software
                                .map(p -> getTypedRow(communityIdList, p.getContext(), p.getId(),"software"))
                                .filter(p -> !(p == null))
                                .mapToPair(toPair())
                        )
                        .union(other
                                .map(p -> getTypedRow(communityIdList, p.getContext(), p.getId(),"otherresearchproduct"))
                                .filter(p -> !(p == null))
                                .mapToPair(toPair())
                        );

                JavaPairRDD<String, TypedRow> to_add_result_communities = resultLinkedToCommunities.join(result_result).map(r -> r._2()._1().setSourceId(r._2()._2().getTargetId()))
                        .mapToPair(toPair());

                JavaPairRDD<String, Result> pubs = publications.mapToPair(p -> new Tuple2<>(p.getId(),p));
                JavaPairRDD<String, Result> dss = datasets.mapToPair(p -> new Tuple2<>(p.getId(),p));
                JavaPairRDD<String, Result> sfw = software.mapToPair(p -> new Tuple2<>(p.getId(),p));
                JavaPairRDD<String, Result> orp = other.mapToPair(p -> new Tuple2<>(p.getId(),p));

                updateResultForCommunity(pubs, to_add_result_communities, outputPath, "publication", PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_ID, PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_NAME);
                updateResultForCommunity(dss, to_add_result_communities, outputPath, "dataset", PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_ID, PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_NAME);
                updateResultForCommunity(sfw, to_add_result_communities, outputPath, "software", PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_ID, PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_NAME);
                updateResultForCommunity(orp, to_add_result_communities, outputPath, "otherresearchproduct", PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_ID, PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_NAME);
                //leftouterjoin result.to_add_result_communities (result = java pair rdd result) [left outer join perche' li voglio tutti anche quelli che non ho aggiornato]
                //per quelli che matchano cercare nel risultato se i context da aggiungere sono gia' presenti. Se non ci sono aggiungerli, altrimenti nulla
        */
    }

    private static org.apache.spark.sql.Dataset<Row> getUpdateCommunitiesForTable(
            SparkSession spark, String table) {
        String query =
                "SELECT target_id, collect_set(co.id) context_id  "
                        + "       FROM (SELECT t.id target_id, s.context source_context "
                        + "             FROM context_software s "
                        + "             JOIN "
                        + table
                        + " t "
                        + "             ON s.target = t.id "
                        + "             UNION ALL "
                        + "             SELECT t.id target_id, d.context source_context "
                        + "             FROM dataset_context d  "
                        + "             JOIN "
                        + table
                        + " t"
                        + "             ON s.target = t.id  "
                        + "             UNION ALL  "
                        + "             SELECT t.id target_id, p.context source_context "
                        + "             FROM publication_context p"
                        + "             JOIN "
                        + table
                        + " t "
                        + "             on p.target = t.id "
                        + "             UNION ALL "
                        + "             SELECT t.id target_id, o.context source_context "
                        + "             FROM other_context o "
                        + "             JOIN "
                        + table
                        + " t  "
                        + "             ON o.target = t.id)  TMP "
                        + "             LATERAL VIEW EXPLODE(source_context) MyT as co "
                        + "             GROUP BY target_id";

        return spark.sql(query);
    }

    private static JavaRDD<Result> createUpdateForResultDatasetWrite(
            JavaRDD<Row> toupdateresult,
            String outputPath,
            String type,
            String class_id,
            String class_name,
            List<String> communityIdList) {
        return toupdateresult
                .map(
                        r -> {
                            List<Context> contextList = new ArrayList();
                            List<String> toAddContext = r.getList(1);
                            for (String cId : toAddContext) {
                                if (communityIdList.contains(cId)) {
                                    Context newContext = new Context();
                                    newContext.setId(cId);
                                    newContext.setDataInfo(
                                            Arrays.asList(
                                                    getDataInfo(
                                                            PROPAGATION_DATA_INFO_TYPE,
                                                            class_id,
                                                            class_name)));
                                    contextList.add(newContext);
                                }
                            }

                            if (contextList.size() > 0) {
                                Result ret = new Result();
                                ret.setId(r.getString(0));
                                ret.setContext(contextList);
                                return ret;
                            }
                            return null;
                        })
                .filter(r -> r != null);
    }

    private static void updateForSoftwareDataset(
            JavaRDD<Row> toupdateresult,
            JavaRDD<Software> result,
            String outputPath,
            String type,
            String class_id,
            String class_name,
            List<String> communityIdList) {
        JavaPairRDD<String, Result> tmp = result.mapToPair(r -> new Tuple2(r.getId(), r));
        getUpdateForResultDataset(
                        toupdateresult,
                        tmp,
                        outputPath,
                        type,
                        class_id,
                        class_name,
                        communityIdList)
                .map(r -> (Software) r)
                .map(s -> new ObjectMapper().writeValueAsString(s))
                .saveAsTextFile(outputPath + "/" + type);
    }

    private static void updateForDatasetDataset(
            JavaRDD<Row> toupdateresult,
            JavaRDD<Dataset> result,
            String outputPath,
            String type,
            String class_id,
            String class_name,
            List<String> communityIdList) {
        JavaPairRDD<String, Result> tmp = result.mapToPair(r -> new Tuple2(r.getId(), r));
        getUpdateForResultDataset(
                        toupdateresult,
                        tmp,
                        outputPath,
                        type,
                        class_id,
                        class_name,
                        communityIdList)
                .map(r -> (Dataset) r)
                .map(d -> new ObjectMapper().writeValueAsString(d))
                .saveAsTextFile(outputPath + "/" + type);
    }

    private static void updateForPublicationDataset(
            JavaRDD<Row> toupdateresult,
            JavaRDD<Publication> result,
            String outputPath,
            String type,
            String class_id,
            String class_name,
            List<String> communityIdList) {
        JavaPairRDD<String, Result> tmp = result.mapToPair(r -> new Tuple2(r.getId(), r));
        getUpdateForResultDataset(
                        toupdateresult,
                        tmp,
                        outputPath,
                        type,
                        class_id,
                        class_name,
                        communityIdList)
                .map(r -> (Publication) r)
                .map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath + "/" + type);
    }

    private static void updateForOtherDataset(
            JavaRDD<Row> toupdateresult,
            JavaRDD<OtherResearchProduct> result,
            String outputPath,
            String type,
            String class_id,
            String class_name,
            List<String> communityIdList) {
        JavaPairRDD<String, Result> tmp = result.mapToPair(r -> new Tuple2(r.getId(), r));
        getUpdateForResultDataset(
                        toupdateresult,
                        tmp,
                        outputPath,
                        type,
                        class_id,
                        class_name,
                        communityIdList)
                .map(r -> (OtherResearchProduct) r)
                .map(o -> new ObjectMapper().writeValueAsString(o))
                .saveAsTextFile(outputPath + "/" + type);
    }

    private static JavaRDD<Result> getUpdateForResultDataset(
            JavaRDD<Row> toupdateresult,
            JavaPairRDD<String, Result> result,
            String outputPath,
            String type,
            String class_id,
            String class_name,
            List<String> communityIdList) {
        return result.leftOuterJoin(
                        toupdateresult.mapToPair(r -> new Tuple2<>(r.getString(0), r.getList(1))))
                .map(
                        c -> {
                            if (!c._2()._2().isPresent()) {
                                return c._2()._1();
                            }

                            List<Object> toAddContext = c._2()._2().get();
                            Set<String> context_set = new HashSet<>();
                            for (Object cId : toAddContext) {
                                String id = (String) cId;
                                if (communityIdList.contains(id)) {
                                    context_set.add(id);
                                }
                            }
                            for (Context context : c._2()._1().getContext()) {
                                if (context_set.contains(context)) {
                                    context_set.remove(context);
                                }
                            }

                            List<Context> contextList =
                                    context_set.stream()
                                            .map(
                                                    co -> {
                                                        Context newContext = new Context();
                                                        newContext.setId(co);
                                                        newContext.setDataInfo(
                                                                Arrays.asList(
                                                                        getDataInfo(
                                                                                PROPAGATION_DATA_INFO_TYPE,
                                                                                class_id,
                                                                                class_name)));
                                                        return newContext;
                                                    })
                                            .collect(Collectors.toList());

                            if (contextList.size() > 0) {
                                Result r = new Result();
                                r.setId(c._1());
                                r.setContext(contextList);
                                return r;
                            }
                            return null;
                        })
                .filter(r -> r != null);

        //        return toupdateresult.mapToPair(r -> new Tuple2<>(r.getString(0), r.getList(1)))
        //                .join(result)
        //                .map(c -> {
        //                    List<Object> toAddContext = c._2()._1();
        //                    Set<String> context_set = new HashSet<>();
        //                    for(Object cId: toAddContext){
        //                        String id = (String)cId;
        //                        if (communityIdList.contains(id)){
        //                            context_set.add(id);
        //                        }
        //                    }
        //                    for (Context context:  c._2()._2().getContext()){
        //                        if(context_set.contains(context)){
        //                            context_set.remove(context);
        //                        }
        //                    }
        //
        //                    List<Context> contextList = context_set.stream().map(co -> {
        //                        Context newContext = new Context();
        //                        newContext.setId(co);
        //
        // newContext.setDataInfo(Arrays.asList(getDataInfo(PROPAGATION_DATA_INFO_TYPE, class_id,
        // class_name)));
        //                       return newContext;
        //
        //                    }).collect(Collectors.toList());
        //
        //                    if(contextList.size() > 0 ){
        //                        Result r = new Result();
        //                        r.setId(c._1());
        //                        r.setContext(contextList);
        //                        return r;
        //                    }
        //                   return null;
        //                })
        //                .filter(r -> r != null);
    }

    private static JavaRDD<Software> createUpdateForSoftwareDataset(
            JavaRDD<Row> toupdateresult,
            List<String> communityList,
            JavaRDD<Software> result,
            String class_id,
            String class_name) {
        return result.mapToPair(s -> new Tuple2<>(s.getId(), s))
                .leftOuterJoin(getStringResultJavaPairRDD(toupdateresult, communityList))
                .map(
                        c -> {
                            Software oaf = c._2()._1();
                            if (c._2()._2().isPresent()) {

                                HashSet<String> contexts = new HashSet<>(c._2()._2().get());

                                for (Context context : oaf.getContext()) {
                                    if (contexts.contains(context.getId())) {
                                        if (!context.getDataInfo().stream()
                                                .map(di -> di.getInferenceprovenance())
                                                .collect(Collectors.toSet())
                                                .contains(PROPAGATION_DATA_INFO_TYPE)) {
                                            context.getDataInfo()
                                                    .add(
                                                            getDataInfo(
                                                                    PROPAGATION_DATA_INFO_TYPE,
                                                                    class_id,
                                                                    class_name));
                                            // community id already in the context of the result.
                                            // Remove it from the set that has to be added
                                            contexts.remove(context.getId());
                                        }
                                    }
                                }
                                List<Context> cc = oaf.getContext();
                                for (String cId : contexts) {
                                    Context context = new Context();
                                    context.setId(cId);
                                    context.setDataInfo(
                                            Arrays.asList(
                                                    getDataInfo(
                                                            PROPAGATION_DATA_INFO_TYPE,
                                                            class_id,
                                                            class_name)));
                                    cc.add(context);
                                }
                                oaf.setContext(cc);
                            }
                            return oaf;
                        });
    }

    private static JavaPairRDD<String, List<String>> getStringResultJavaPairRDD(
            JavaRDD<Row> toupdateresult, List<String> communityList) {
        return toupdateresult.mapToPair(
                c -> {
                    List<String> contextList = new ArrayList<>();
                    List<String> contexts = c.getList(1);
                    for (String context : contexts) {
                        if (communityList.contains(context)) {
                            contextList.add(context);
                        }
                    }

                    return new Tuple2<>(c.getString(0), contextList);
                });
    }

    private static org.apache.spark.sql.Dataset<Row> getContext(SparkSession spark, String table) {
        String query =
                "SELECT relation.source, "
                        + table
                        + ".context , relation.target "
                        + "FROM "
                        + table
                        + " JOIN relation  "
                        + "ON  id = source";

        return spark.sql(query);
    }

    private static Boolean relatedToCommunities(Result r, List<String> communityIdList) {
        Set<String> result_communities =
                r.getContext().stream().map(c -> c.getId()).collect(Collectors.toSet());
        for (String communityId : result_communities) {
            if (communityIdList.contains(communityId)) {
                return true;
            }
        }
        return false;
    }

    private static void updateResult(
            JavaPairRDD<String, Result> results,
            JavaPairRDD<String, TypedRow> toupdateresult,
            String outputPath,
            String type) {
        results.leftOuterJoin(toupdateresult)
                .map(
                        p -> {
                            Result r = p._2()._1();
                            if (p._2()._2().isPresent()) {
                                Set<String> communityList = p._2()._2().get().getAccumulator();
                                for (Context c : r.getContext()) {
                                    if (communityList.contains(c.getId())) {
                                        // verify if the datainfo for this context contains
                                        // propagation
                                        if (!c.getDataInfo().stream()
                                                .map(di -> di.getInferenceprovenance())
                                                .collect(Collectors.toSet())
                                                .contains(PROPAGATION_DATA_INFO_TYPE)) {
                                            c.getDataInfo()
                                                    .add(
                                                            getDataInfo(
                                                                    PROPAGATION_DATA_INFO_TYPE,
                                                                    PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_ID,
                                                                    PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_NAME));
                                            // community id already in the context of the result.
                                            // Remove it from the set that has to be added
                                            communityList.remove(c.getId());
                                        }
                                    }
                                }
                                List<Context> cc = r.getContext();
                                for (String cId : communityList) {
                                    Context context = new Context();
                                    context.setId(cId);
                                    context.setDataInfo(
                                            Arrays.asList(
                                                    getDataInfo(
                                                            PROPAGATION_DATA_INFO_TYPE,
                                                            PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_ID,
                                                            PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_NAME)));
                                    cc.add(context);
                                }
                                r.setContext(cc);
                            }
                            return r;
                        })
                .map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath + "/" + type);
    }

    private static TypedRow getTypedRow(
            List<String> communityIdList, List<Context> context, String id, String type) {
        Set<String> result_communities =
                context.stream().map(c -> c.getId()).collect(Collectors.toSet());
        TypedRow tp = new TypedRow();
        tp.setSourceId(id);
        tp.setType(type);
        for (String communityId : result_communities) {
            if (communityIdList.contains(communityId)) {
                tp.add(communityId);
            }
        }
        if (tp.getAccumulator() != null) {
            return tp;
        }
        return null;
    }
}
