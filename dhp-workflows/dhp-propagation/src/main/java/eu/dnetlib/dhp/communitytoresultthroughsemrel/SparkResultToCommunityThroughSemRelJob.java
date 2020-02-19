package eu.dnetlib.dhp.communitytoresultthroughsemrel;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.TypedRow;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.resulttoorganizationfrominstrepo.SparkResultToOrganizationFromIstRepoJob;
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
import java.util.List;
import java.util.Set;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.PropagationConstant.PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME;

public class SparkResultToCommunityThroughSemRelJob {
    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkResultToCommunityThroughSemRelJob.class.getResourceAsStream("/eu/dnetlib/dhp/communitytoresultthroughsemrel/input_communitytoresultthroughsemrel_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkResultToCommunityThroughSemRelJob.class.getSimpleName())
                .master(parser.get("master"))
                .enableHiveSupport()
                .getOrCreate();

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String inputPath = parser.get("sourcePath");
        final String outputPath = "/tmp/provision/propagation/communitytoresultthroughsemrel";

        final List<String> allowedsemrel = Arrays.asList(parser.get("allowedsemrel").split(";"));
        final List<String> communityIdList = Arrays.asList(parser.get("communityidlist").split(";"));

        File directory = new File(outputPath);

        if (!directory.exists()) {
            directory.mkdirs();
        }
/*
        //get the institutional repositories
        JavaPairRDD<String, TypedRow> datasources = sc.sequenceFile(inputPath + "/datasource", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Datasource.class))
                .filter(ds -> INSTITUTIONAL_REPO_TYPE.equals(ds.getDatasourcetype().getClassid()))
                .map(ds -> new TypedRow().setSourceId(ds.getId()))
                .mapToPair(toPair());


        JavaPairRDD<String, TypedRow> rel_datasource_organization = sc.sequenceFile(inputPath + "/relation", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Relation.class))
                .filter(r -> !r.getDataInfo().getDeletedbyinference())
                .filter(r -> RELATION_DATASOURCEORGANIZATION_REL_TYPE.equals(r.getRelClass()) && RELATION_DATASOURCE_ORGANIZATION_REL_CLASS.equals(r.getRelType()))
                .map(r -> new TypedRow().setSourceId(r.getSource()).setTargetId(r.getTarget()))
                .mapToPair(toPair());

        JavaPairRDD<String, TypedRow> instdatasource_organization = datasources.join(rel_datasource_organization)
                .map(x -> x._2()._2())
                .mapToPair(toPair());

        JavaRDD<Relation> relations = sc.sequenceFile(inputPath + "/relation", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Relation.class));
        JavaRDD<Publication> publications = sc.sequenceFile(inputPath + "/publication", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Publication.class));
        JavaRDD<Dataset> datasets = sc.sequenceFile(inputPath + "/dataset", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Dataset.class));
        JavaRDD<Software> software = sc.sequenceFile(inputPath + "/software", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Software.class));
        JavaRDD<OtherResearchProduct> other = sc.sequenceFile(inputPath + "/otherresearchproduct", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), OtherResearchProduct.class));

        JavaPairRDD<String, TypedRow> datasource_results = publications
                .map(oaf -> getTypedRowsDatasourceResult(oaf))
                .flatMapToPair(f -> {
                    ArrayList<Tuple2<String, TypedRow>> ret = new ArrayList<>();
                    for (TypedRow t : f) {
                        ret.add(new Tuple2<>(t.getSourceId(), t));
                    }
                    return ret.iterator();
                })
                .union(datasets
                        .map(oaf -> getTypedRowsDatasourceResult(oaf))
                        .flatMapToPair(f -> {
                            ArrayList<Tuple2<String, TypedRow>> ret = new ArrayList<>();
                            for (TypedRow t : f) {
                                ret.add(new Tuple2<>(t.getSourceId(), t));
                            }
                            return ret.iterator();
                        }))
                .union(software
                        .map(oaf -> getTypedRowsDatasourceResult(oaf))
                        .flatMapToPair(f -> {
                            ArrayList<Tuple2<String, TypedRow>> ret = new ArrayList<>();
                            for (TypedRow t : f) {
                                ret.add(new Tuple2<>(t.getSourceId(), t));
                            }
                            return ret.iterator();
                        }))
                .union(other
                        .map(oaf -> getTypedRowsDatasourceResult(oaf))
                        .flatMapToPair(f -> {
                            ArrayList<Tuple2<String, TypedRow>> ret = new ArrayList<>();
                            for (TypedRow t : f) {
                                ret.add(new Tuple2<>(t.getSourceId(), t));
                            }
                            return ret.iterator();
                        }));

        JavaRDD<Relation> newRels = instdatasource_organization.join(datasource_results)
                .flatMap(c -> {
                    List<Relation> rels = new ArrayList();
                    String orgId = c._2()._1().getTargetId();
                    String resId = c._2()._2().getTargetId();
                    rels.add(getRelation(orgId, resId, RELATION_ORGANIZATION_RESULT_REL_CLASS,
                            RELATION_RESULTORGANIZATION_REL_TYPE, RELATION_RESULTORGANIZATION_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
                            PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID, PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME));
                    rels.add(getRelation(resId, orgId, RELATION_RESULT_ORGANIZATION_REL_CLASS,
                            RELATION_RESULTORGANIZATION_REL_TYPE, RELATION_RESULTORGANIZATION_SUBREL_TYPE, PROPAGATION_DATA_INFO_TYPE,
                            PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_ID, PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME));
                    return rels.iterator();
                });
        newRels.map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath + "/relation_new");

        newRels.union(relations).map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath + "/relation");*/
    }
}
/*
package eu.dnetlib.data.mapreduce.hbase.propagation.communitytoresult;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import eu.dnetlib.data.mapreduce.hbase.propagation.Value;
import eu.dnetlib.data.mapreduce.util.OafRowKeyDecoder;
import eu.dnetlib.data.proto.OafProtos;
import eu.dnetlib.data.proto.ResultProtos;
import eu.dnetlib.data.proto.TypeProtos;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.stream.Collectors;

import static eu.dnetlib.data.mapreduce.hbase.propagation.PropagationConstants.COUNTER_PROPAGATION;
import static eu.dnetlib.data.mapreduce.hbase.propagation.PropagationConstants.DEFAULT_COMMUNITY_RELATION_SET;
import static eu.dnetlib.data.mapreduce.hbase.propagation.Utils.getEntity;
import static eu.dnetlib.data.mapreduce.hbase.propagation.Utils.getRelationTarget;


public class CommunityToResultMapper extends TableMapper<Text, Text> {

    private Text keyOut;
    private Text valueOut;
    private String[] sem_rels;
    private String trust;
    CommunityList idCommunityList;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {

        idCommunityList = new Gson().fromJson(context.getConfiguration().get("community.id.list"), CommunityList.class);
        keyOut = new Text();
        valueOut = new Text();

        sem_rels = context.getConfiguration().getStrings("propagatetocommunity.semanticrelations", DEFAULT_COMMUNITY_RELATION_SET);
        trust = context.getConfiguration().get("propagatetocommunity.trust","0.85");

    }

    @Override
    protected void map(final ImmutableBytesWritable keyIn, final Result value, final Context context) throws IOException, InterruptedException {

        final TypeProtos.Type type = OafRowKeyDecoder.decode(keyIn.copyBytes()).getType();

        //If the type is not result I do not need to process it
        if (!type.equals(TypeProtos.Type.result)) {
            return;
        }

        //verify if entity is valid
        final OafProtos.OafEntity entity = getEntity(value, type);
        if (entity == null) {
            context.getCounter(COUNTER_PROPAGATION, "Del by inference or null body for result").increment(1);
            return;
        }
        final Set<String> toemitrelations = new HashSet<>();
        //verify if we have some relation
        for (String sem_rel : sem_rels)
            toemitrelations.addAll(getRelationTarget(value, sem_rel, context, COUNTER_PROPAGATION));

        if (toemitrelations.isEmpty()) {
            context.getCounter(COUNTER_PROPAGATION, "No allowed semantic relation present in result").increment(1);
            return;
        }

        //verify if we have a relation to a context in the body
        Set<String> contextIds = entity.getResult().getMetadata().getContextList()
                .stream()
                .map(ResultProtos.Result.Context::getId)
                .collect(Collectors.toSet());

        //verify if we have a relation to a context in the update part made by the inference
        NavigableMap<byte[], byte[]> map = value.getFamilyMap(Bytes.toBytes(TypeProtos.Type.result.toString()));

        final Map<String, byte[]> stringMap = Maps.newHashMap();
        for (Map.Entry<byte[], byte[]> e : map.entrySet()) {
            stringMap.put(Bytes.toString(e.getKey()), e.getValue());
        }

        // we fetch all the body updates
        for (final String o : stringMap.keySet()) {
            if (o.startsWith("update_")) {
                final OafProtos.Oaf update = OafProtos.Oaf.parseFrom(stringMap.get(o));
                contextIds.addAll(update.getEntity().getResult().getMetadata().getContextList()
                        .stream()
                        .map(ResultProtos.Result.Context::getId)
                        .map(s -> s.split("::")[0])
                        .collect(Collectors.toSet()));
            }
        }

        //we verify if we have something
        if (contextIds.isEmpty()) {
            context.getCounter(COUNTER_PROPAGATION, "No context in the body and in the update of the result").increment(1);
            return;
        }

        //verify if some of the context collected for the result are associated to a community in the communityIdList
        for (String id : idCommunityList) {
            if (contextIds.contains(id)) {
                for (String target : toemitrelations) {
                    keyOut.set(target);
                    valueOut.set(Value.newInstance(id).setTrust(trust).toJson());
                    context.write(keyOut, valueOut);
                    context.getCounter(COUNTER_PROPAGATION, "Emit propagation for " + id).increment(1);
                }
            }
        }

    }

}


 */