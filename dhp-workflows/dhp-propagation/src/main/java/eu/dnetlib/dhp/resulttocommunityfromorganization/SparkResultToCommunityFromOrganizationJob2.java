package eu.dnetlib.dhp.resulttocommunityfromorganization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import eu.dnetlib.dhp.TypedRow;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
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

import java.util.*;
import java.util.stream.Collectors;

import static eu.dnetlib.dhp.PropagationConstant.*;

public class SparkResultToCommunityFromOrganization {

    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkResultToCommunityFromOrganization.class.getResourceAsStream("/eu/dnetlib/dhp/resulttocommunityfromorganization/input_communitytoresult_parameters.json")));
        parser.parseArgument(args);
        SparkConf conf = new SparkConf();
        conf.set("hive.metastore.uris", parser.get("hive_metastore_uris"));
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkResultToCommunityFromOrganization.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("sourcePath");
        final String outputPath = "/tmp/provision/propagation/communitytoresult";
        final OrganizationMap organizationMap = new Gson().fromJson(parser.get("organizationtoresultcommunitymap"), OrganizationMap.class);
        boolean writeUpdates = TRUE.equals(parser.get("writeUpdate"));
        boolean saveGraph = TRUE.equals(parser.get("saveGraph"));

        System.out.println(new Gson().toJson(organizationMap));

        createOutputDirs(outputPath, FileSystem.get(spark.sparkContext().hadoopConfiguration()));

        JavaRDD<Relation> relations_rdd_all = sc.textFile(inputPath + "/relation")
                .map(item -> new ObjectMapper().readValue(item, Relation.class));
        JavaRDD<Publication> publications_rdd_all = sc.textFile(inputPath + "/publication")
                .map(item -> new ObjectMapper().readValue(item, Publication.class));
        JavaRDD<Dataset> dataset_rdd_all = sc.textFile(inputPath + "/dataset")
                .map(item -> new ObjectMapper().readValue(item, Dataset.class));
        JavaRDD<OtherResearchProduct> orp_rdd_all = sc.textFile(inputPath + "/otheresearchproduct")
                .map(item -> new ObjectMapper().readValue(item, OtherResearchProduct.class));
        JavaRDD<Software> software_rdd_all = sc.textFile(inputPath + "/software")
                .map(item -> new ObjectMapper().readValue(item, Software.class));

        org.apache.spark.sql.Dataset<Relation> relation = spark.createDataset(relations_rdd_all.rdd(), Encoders.bean(Relation.class));

        relation.createOrReplaceTempView("relation");
//        String query = "SELECT source, target" +
//                "       FROM relation " +
//                "       WHERE datainfo.deletedbyinference = false " +
//                "       AND relClass = '" + RELATION_RESULT_ORGANIZATION_REL_CLASS + "'";
//
//        org.apache.spark.sql.Dataset<Row> result_organization = spark.sql(query);
//        result_organization.createOrReplaceTempView("result_organization");
//
//        query = "SELECT source, target " +
//                "FROM relation " +
//                "WHERE datainfo.deletedbyinference = false " +
//                "AND relClass = '" + RELATION_REPRESENTATIVERESULT_RESULT_CLASS + "'" ;

        String query = "SELECT result_organization.source, result_organization.target, org_set  " +
                "FROM (SELECT source, target " +
                "      FROM relation " +
                "      WHERE datainfo.deletedbyinference = false " +
                "      AND relClass = '" + RELATION_RESULT_ORGANIZATION_REL_CLASS + "') result_organization " +
                "LEFT JOIN (SELECT source, collect_set(target) org_set " +
                "      FROM relation " +
                "      WHERE datainfo.deletedbyinference = false " +
                "      AND relClass = '" + RELATION_REPRESENTATIVERESULT_RESULT_CLASS + "' " +
                "      GROUP BY source) organization_organization " +
                "ON result_organization.target = organization_organization.source ";

        org.apache.spark.sql.Dataset<Row> result_organizationset = spark.sql(query);

        JavaPairRDD<String,TypedRow> result_communitySet = result_organizationset.toJavaRDD().map(r -> {
            String rId = r.getString(0);
            List<String> orgs = r.getList(2);
            String oTarget = r.getString(1);
            TypedRow tp = new TypedRow();
            if (organizationMap.containsKey(oTarget)) {
                    tp.addAll(organizationMap.get(oTarget));
            }
            try{
                for (String oId : orgs) {
                    if (organizationMap.containsKey(oId)) {
                        tp.addAll(organizationMap.get(oId));
                    }
                }
            }
            catch(Exception e){
                //System.out.println("organizationTargetID: " + oTarget);
            }

            if(tp.getAccumulator() == null ){
                return null;
            }
            tp.setSourceId(rId);


            return tp;
        })
                .filter(tr -> !(tr==null))
                .mapToPair(toPair()).cache();

        if(writeUpdates){
            result_communitySet.map(c->new ObjectMapper().writeValueAsString(c)).saveAsTextFile(outputPath +"/result_communityset");
        }

        if(saveGraph){
            updatePublicationResult(publications_rdd_all, result_communitySet)
                    .map(p -> new ObjectMapper().writeValueAsString(p))
                    .saveAsTextFile(outputPath + "/publication");

            updateDatasetResult(dataset_rdd_all, result_communitySet)
                    .map(p -> new ObjectMapper().writeValueAsString(p))
                    .saveAsTextFile(outputPath + "/dataset");

            updateORPResult(orp_rdd_all, result_communitySet)
                    .map(p -> new ObjectMapper().writeValueAsString(p))
                    .saveAsTextFile(outputPath + "/otherresearchproduct");

            updateSoftwareResult(software_rdd_all, result_communitySet)
                    .map(p -> new ObjectMapper().writeValueAsString(p))
                    .saveAsTextFile(outputPath + "/software");
        }


        //relations between organziations and results
//        JavaPairRDD<String, TypedRow> organization_result = relations
//                .filter(r -> !r.getDataInfo().getDeletedbyinference())
//                .filter(r -> RELATION_RESULT_ORGANIZATION_REL_CLASS.equals(r.getRelClass())
//                        && RELATION_RESULTORGANIZATION_REL_TYPE.equals(r.getRelType()))
//                .map(r -> {
//                    TypedRow tp = new TypedRow();
//                    tp.setSourceId(r.getTarget());
//                    tp.setTargetId(r.getSource() );
//                    return tp;
//                })
//                .mapToPair(toPair());

        //relations between representative organization and merged Id. One relation per merged organization
//        JavaPairRDD<String, TypedRow> organization_organization = relations
//                .filter(r -> !r.getDataInfo().getDeletedbyinference())
//                .filter( r -> RELATION_ORGANIZATIONORGANIZATION_REL_TYPE.equals(r.getRelType())
//                        && RELATION_REPRESENTATIVERESULT_RESULT_CLASS.equals(r.getRelClass()))
//                .map(r -> {
//                    TypedRow tp = new TypedRow();
//                    tp.setSourceId(r.getSource());
//                    tp.setTargetId(r.getTarget());
//                    return tp;
//                })
//                .mapToPair(toPair());

        //get the original id of the organizations to be checked against the id associated to the communities
//        JavaPairRDD<String, TypedRow> organizationoriginal_result = organization_result.leftOuterJoin(organization_organization)
//                .map(c -> {
//                    if (!c._2()._2().isPresent())
//                        return c._2()._1();
//                    return c._2()._1().setSourceId(c._2()._2().get().getTargetId());
//                })
//                .mapToPair(toPair());

        //associates to each result connected to an organization the list of communities related to that organization
//        JavaPairRDD<String, TypedRow> to_add_result_communities = organizationoriginal_result.map(o -> {
//            List<String> communityList = organizationMap.get(o._1());
//            if (communityList.size() == 0)
//                return null;
//            TypedRow tp = o._2();
//            tp.setAccumulator(new HashSet<>(communityList));
//            tp.setSourceId(tp.getTargetId());
//            return tp;
//        })
//                .filter(r -> !(r == null))
//                .mapToPair(toPair())
//                .reduceByKey((a, b) -> {
//                    if (a == null)
//                        return b;
//                    if (b == null)
//                        return a;
//                    a.addAll(b.getAccumulator());
//                    return a;
//                });


//        JavaRDD<Publication> publications = sc.sequenceFile(inputPath + "/publication", Text.class, Text.class)
//                .map(item -> new ObjectMapper().readValue(item._2().toString(), Publication.class)).cache();
//        JavaRDD<Dataset> datasets = sc.sequenceFile(inputPath + "/dataset", Text.class, Text.class)
//                .map(item -> new ObjectMapper().readValue(item._2().toString(), Dataset.class)).cache();
//        JavaRDD<Software> software = sc.sequenceFile(inputPath + "/software", Text.class, Text.class)
//                .map(item -> new ObjectMapper().readValue(item._2().toString(), Software.class)).cache();
//        JavaRDD<OtherResearchProduct> other = sc.sequenceFile(inputPath + "/otherresearchproduct", Text.class, Text.class)
//                .map(item -> new ObjectMapper().readValue(item._2().toString(), OtherResearchProduct.class)).cache();
//
//        JavaPairRDD<String, Result> pubs = publications.mapToPair(p -> new Tuple2<>(p.getId(),p));
//        JavaPairRDD<String, Result> dss = datasets.mapToPair(p -> new Tuple2<>(p.getId(),p));
//        JavaPairRDD<String, Result> sfw = software.mapToPair(p -> new Tuple2<>(p.getId(),p));
//        JavaPairRDD<String, Result> orp = other.mapToPair(p -> new Tuple2<>(p.getId(),p));
//
//        updateResultForCommunity(pubs, to_add_result_communities, outputPath, "publication", PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_ID, PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_NAME);
//        updateResultForCommunity(dss, to_add_result_communities, outputPath, "dataset", PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_ID, PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_NAME);
//        updateResultForCommunity(sfw, to_add_result_communities, outputPath, "software", PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_ID, PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_NAME);
//        updateResultForCommunity(orp, to_add_result_communities, outputPath, "otherresearchproduct", PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_ID, PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_NAME);

    }

    private static JavaRDD<Publication> updatePublicationResult(JavaRDD<Publication> result, JavaPairRDD<String, TypedRow> result_communitySet){
        JavaRDD<Result> tmp = result.map(r -> (Result)r);
        return updateResultDataset(tmp, result_communitySet).map(r -> (Publication)r);

    }
    private static JavaRDD<Dataset> updateDatasetResult(JavaRDD<Dataset> result, JavaPairRDD<String, TypedRow> result_communitySet){
        JavaRDD<Result> tmp = result.map(r -> (Result)r);
        return updateResultDataset(tmp, result_communitySet).map(r -> (Dataset) r);

    }
    private static JavaRDD<OtherResearchProduct> updateORPResult(JavaRDD<OtherResearchProduct> result, JavaPairRDD<String, TypedRow> result_communitySet){
        JavaRDD<Result> tmp = result.map(r -> (Result)r);
        return updateResultDataset(tmp, result_communitySet).map(r -> (OtherResearchProduct)r);

    }
    private static JavaRDD<Software> updateSoftwareResult(JavaRDD<Software> result, JavaPairRDD<String, TypedRow> result_communitySet){
        JavaRDD<Result> tmp = result.map(r -> (Result)r);
        return updateResultDataset(tmp, result_communitySet).map(r -> (Software) r);

    }
    private  static JavaRDD<Result> updateResultDataset(JavaRDD<Result> result, JavaPairRDD<String, TypedRow> result_communitySet){
        return result.mapToPair(p -> new Tuple2<>(p.getId(),p)).leftOuterJoin(result_communitySet)
                .map(c -> {
                    Result r = c._2()._1();
                    if(c._2()._2().isPresent()){
                        Set<String> communitySet = c._2()._2().get().getAccumulator();
                        List<String> contextList = r.getContext().stream().map(con -> con.getId()).collect(Collectors.toList());
                        for(String cId:communitySet){
                            if(!contextList.contains(cId)){
                                Context newContext = new Context();
                                newContext.setId(cId);
                                newContext.setDataInfo(Arrays.asList(getDataInfo(PROPAGATION_DATA_INFO_TYPE,
                                        PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_ID, PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_NAME)));
                                r.getContext().add(newContext);
                            }
                        }
                    }
                    return r;
                });

    }
    //   private static void updateResult(JavaPairRDD<String, Result> results, JavaPairRDD<String, TypedRow> toupdateresult, String outputPath, String type) {
//        results.leftOuterJoin(toupdateresult)
//                .map(p -> {
//                    Result r = p._2()._1();
//                    if (p._2()._2().isPresent()){
//                        Set<String> communityList = p._2()._2().get().getAccumulator();
//                        for(Context c: r.getContext()){
//                            if (communityList.contains(c.getId())){
//                                //verify if the datainfo for this context contains propagation
//                                if (!c.getDataInfo().stream().map(di -> di.getInferenceprovenance()).collect(Collectors.toSet()).contains(PROPAGATION_DATA_INFO_TYPE)){
//                                    c.getDataInfo().add(getDataInfo(PROPAGATION_DATA_INFO_TYPE, PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_ID, PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_NAME));
//                                    //community id already in the context of the result. Remove it from the set that has to be added
//                                    communityList.remove(c.getId());
//                                }
//                            }
//                        }
//                        List<Context> cc = r.getContext();
//                        for(String cId: communityList){
//                            Context context = new Context();
//                            context.setId(cId);
//                            context.setDataInfo(Arrays.asList(getDataInfo(PROPAGATION_DATA_INFO_TYPE, PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_ID, PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_NAME)));
//                            cc.add(context);
//                        }
//                        r.setContext(cc);
//                    }
//                    return r;
//                })
//                .map(p -> new ObjectMapper().writeValueAsString(p))
//                .saveAsTextFile(outputPath+"/"+type);
//    }


}


/*
package eu.dnetlib.data.mapreduce.hbase.propagation.communitythroughorganization;

import com.google.gson.Gson;
import eu.dnetlib.data.mapreduce.hbase.propagation.Value;
import eu.dnetlib.data.mapreduce.util.OafRowKeyDecoder;
import eu.dnetlib.data.proto.OafProtos;
import eu.dnetlib.data.proto.TypeProtos;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Set;

import static eu.dnetlib.data.mapreduce.hbase.propagation.PropagationConstants.*;
import static eu.dnetlib.data.mapreduce.hbase.propagation.Utils.getEntity;
import static eu.dnetlib.data.mapreduce.hbase.propagation.Utils.getRelationTarget;

public class PropagationCommunityThroughOrganizationMapper extends TableMapper<ImmutableBytesWritable, Text> {
    private Text valueOut;
    private ImmutableBytesWritable keyOut;
    private OrganizationMap organizationMap;

    //seleziono il tipo della entry:
    //Result:
    //se non e' deleted by inference ed ha organizzazioni a cui e' associato,
    // //emetto id della relazione ed id del risultato per ogni organizzazione a cui e' associato
    //ORGANIZATION:
    //se non e' deleted by inference e non e' deduplicata emetto l'id della organizzazione
    //se non e' deleted by inference ed e' deduplicata, emetto id della organizzazione ed id del deduplicato e lista delle organizzazioni mergiate
    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        valueOut = new Text();
        keyOut = new ImmutableBytesWritable();
        organizationMap = new Gson().fromJson(context.getConfiguration().get("organizationtoresult.community.map"), OrganizationMap.class);
        System.out.println("got organizationtoresult map: " + new Gson().toJson(organizationMap)) ;
    }

    @Override
    protected void map(final ImmutableBytesWritable keyIn, final Result value, final Context context) throws IOException, InterruptedException {
        final TypeProtos.Type type = OafRowKeyDecoder.decode(keyIn.copyBytes()).getType();
        final OafProtos.OafEntity entity = getEntity(value, type);//getEntity already verified that it is not delByInference
        if (entity != null) {
            switch (type) {
                case organization:
                    DedupedList communityList = getCommunityList(Bytes.toString(keyIn.get()),
                            getRelationTarget(value, DEDUP_RELATION_ORGANIZATION + REL_DEDUP_REPRESENTATIVE_RESULT, context, COUNTER_PROPAGATION), context);
                    if (communityList.size() > 0){
                        valueOut.set(Value.newInstance(
                                new Gson().toJson(
                                        communityList, //search for organizationtoresult it merges
                                        DedupedList.class),
                                ORGANIZATION_COMMUNITY_TRUST,
                                Type.fromorganization).toJson());
                        context.write(keyIn, valueOut);
                        context.getCounter(COUNTER_PROPAGATION, "emit for organizationtoresult ").increment(1);
                    }else{
                        context.getCounter(COUNTER_PROPAGATION, "community list size = 0 ").increment(1);
                    }

                    break;
                case result:
                    Set<String> result_organization = getRelationTarget(value, RELATION_ORGANIZATION + REL_RESULT_ORGANIZATION, context, COUNTER_PROPAGATION);
                    for(String org: result_organization)
                        emit(org, Bytes.toString(keyIn.get()), context);
                    break;
            }
        }
    }

    private DedupedList getCommunityList(String organizationId, Set<String> relationTarget, Context context) {
        DedupedList communityList = new DedupedList();
        relationTarget.stream().forEach(org -> communityList.addAll(organizationMap.get(StringUtils.substringAfter(org, "|"))));
        communityList.addAll(organizationMap.get(StringUtils.substringAfter(organizationId,"|")));
        communityList.stream().forEach(c->context.getCounter(COUNTER_PROPAGATION,"found organizationtoresult for " + c).increment(1));
        return communityList;
    }

    private void emit(String org, String resId, Context context) throws IOException, InterruptedException {
        keyOut.set(Bytes.toBytes(org));
        valueOut.set(Value.newInstance(resId, ORGANIZATION_COMMUNITY_TRUST, Type.fromresult).toJson());
        context.write(keyOut,valueOut);
        context.getCounter(COUNTER_PROPAGATION, "emit for result").increment(1);
    }

}
 */