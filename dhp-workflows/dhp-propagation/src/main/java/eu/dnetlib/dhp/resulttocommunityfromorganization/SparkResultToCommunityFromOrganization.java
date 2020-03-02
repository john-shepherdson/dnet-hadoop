package eu.dnetlib.dhp.resulttocommunityfromorganization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import eu.dnetlib.dhp.TypedRow;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.projecttoresult.SparkResultToProjectThroughSemRelJob;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import static eu.dnetlib.dhp.PropagationConstant.*;

public class SparkResultToCommunityFromOrganization {

    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkResultToCommunityFromOrganization.class.getResourceAsStream("/eu/dnetlib/dhp/resulttocommunityfromorganization/input_communitytoresult_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkResultToCommunityFromOrganization.class.getSimpleName())
                .master(parser.get("master"))
                .enableHiveSupport()
                .getOrCreate();

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("sourcePath");
        final String outputPath = "/tmp/provision/propagation/communitytoresult";
        final OrganizationMap organizationMap = new Gson().fromJson(parser.get("organizationtoresultcommunitymap"), OrganizationMap.class);


        File directory = new File(outputPath);

        if (!directory.exists()) {
            directory.mkdirs();
        }

        JavaRDD<Relation> relations = sc.sequenceFile(inputPath + "/relation", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Relation.class)).cache();


        //relations between organziations and results
        JavaPairRDD<String, TypedRow> organization_result = relations
                .filter(r -> !r.getDataInfo().getDeletedbyinference())
                .filter(r -> RELATION_RESULT_ORGANIZATION_REL_CLASS.equals(r.getRelClass()) && RELATION_RESULTORGANIZATION_REL_TYPE.equals(r.getRelType()))
                .map(r -> new TypedRow().setSourceId(r.getTarget()).setTargetId(r.getSource() ))
                .mapToPair(toPair());

        //relations between representative organization and merged Id. One relation per merged organization
        JavaPairRDD<String, TypedRow> organization_organization = relations
                .filter(r -> !r.getDataInfo().getDeletedbyinference())
                .filter( r -> RELATION_ORGANIZATIONORGANIZATION_REL_TYPE.equals(r.getRelType()) && RELATION_REPRESENTATIVERESULT_RESULT_CLASS.equals(r.getRelClass()))
                .map(r -> new TypedRow().setSourceId(r.getSource()).setTargetId(r.getTarget()))
                .mapToPair(toPair());

        //get the original id of the organizations to be checked against the id associated to the communities
        JavaPairRDD<String, TypedRow> organizationoriginal_result = organization_result.leftOuterJoin(organization_organization)
                .map(c -> {
                    if (!c._2()._2().isPresent())
                        return c._2()._1();
                    return c._2()._1().setSourceId(c._2()._2().get().getTargetId());
                })
                .mapToPair(toPair());

        //associates to each result connected to an organization the list of communities related to that organization
        JavaPairRDD<String, TypedRow> to_add_result_communities = organizationoriginal_result.map(o -> {
            List<String> communityList = organizationMap.get(o._1());
            if (communityList.size() == 0)
                return null;
            TypedRow tp = o._2();
            tp.setAccumulator(new HashSet<>(communityList)).setSourceId(tp.getTargetId());
            return tp;
        })
                .filter(r -> !(r == null))
                .mapToPair(toPair())
                .reduceByKey((a, b) -> {
                    if (a == null)
                        return b;
                    if (b == null)
                        return a;
                    a.addAll(b.getAccumulator());
                    return a;
                });


        JavaRDD<Publication> publications = sc.sequenceFile(inputPath + "/publication", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Publication.class)).cache();
        JavaRDD<Dataset> datasets = sc.sequenceFile(inputPath + "/dataset", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Dataset.class)).cache();
        JavaRDD<Software> software = sc.sequenceFile(inputPath + "/software", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Software.class)).cache();
        JavaRDD<OtherResearchProduct> other = sc.sequenceFile(inputPath + "/otherresearchproduct", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), OtherResearchProduct.class)).cache();

        JavaPairRDD<String, Result> pubs = publications.mapToPair(p -> new Tuple2<>(p.getId(),p));
        JavaPairRDD<String, Result> dss = datasets.mapToPair(p -> new Tuple2<>(p.getId(),p));
        JavaPairRDD<String, Result> sfw = software.mapToPair(p -> new Tuple2<>(p.getId(),p));
        JavaPairRDD<String, Result> orp = other.mapToPair(p -> new Tuple2<>(p.getId(),p));

        updateResultForCommunity(pubs, to_add_result_communities, outputPath, "publication", PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_ID, PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_NAME);
        updateResultForCommunity(dss, to_add_result_communities, outputPath, "dataset", PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_ID, PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_NAME);
        updateResultForCommunity(sfw, to_add_result_communities, outputPath, "software", PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_ID, PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_NAME);
        updateResultForCommunity(orp, to_add_result_communities, outputPath, "otherresearchproduct", PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_ID, PROPAGATION_RESULT_COMMUNITY_ORGANIZATION_CLASS_NAME);

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