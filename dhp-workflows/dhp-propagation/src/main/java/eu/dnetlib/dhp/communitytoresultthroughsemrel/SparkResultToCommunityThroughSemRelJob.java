package eu.dnetlib.dhp.communitytoresultthroughsemrel;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.QueryInformationSystem;
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
import java.util.*;
import java.util.stream.Collectors;

import static eu.dnetlib.dhp.PropagationConstant.*;
import static eu.dnetlib.dhp.PropagationConstant.PROPAGATION_RELATION_RESULT_ORGANIZATION_INST_REPO_CLASS_NAME;

public class SparkResultToCommunityThroughSemRelJob {
    public static void main(String[] args) throws Exception {

        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkResultToCommunityThroughSemRelJob.class.getResourceAsStream("/eu/dnetlib/dhp/resulttocommunityfromsemrel/input_propagationresultcommunityfromsemrel_parameters.json")));
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

        final List<String> allowedsemrel = Arrays.asList(parser.get("allowedsemrels").split(";"));
        final List<String> communityIdList = QueryInformationSystem.getCommunityList(parser.get("isLookupUrl"));

        File directory = new File(outputPath);

        if (!directory.exists()) {
            directory.mkdirs();
        }

        JavaPairRDD<String, TypedRow> result_result = getResultResultSemRel(allowedsemrel,
                sc.sequenceFile(inputPath + "/relation", Text.class, Text.class)
                        .map(item -> new ObjectMapper().readValue(item._2().toString(), Relation.class)));

        JavaRDD<Publication> publications = sc.sequenceFile(inputPath + "/publication", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Publication.class)).cache();
        JavaRDD<Dataset> datasets = sc.sequenceFile(inputPath + "/dataset", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Dataset.class)).cache();
        JavaRDD<Software> software = sc.sequenceFile(inputPath + "/software", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), Software.class)).cache();
        JavaRDD<OtherResearchProduct> other = sc.sequenceFile(inputPath + "/otherresearchproduct", Text.class, Text.class)
                .map(item -> new ObjectMapper().readValue(item._2().toString(), OtherResearchProduct.class)).cache();

        JavaPairRDD<String, TypedRow> resultLinkedToCommunities = publications
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

    }

    private static void updateResult(JavaPairRDD<String, Result> results, JavaPairRDD<String, TypedRow> toupdateresult, String outputPath, String type) {
        results.leftOuterJoin(toupdateresult)
                .map(p -> {
                    Result r = p._2()._1();
                    if (p._2()._2().isPresent()){
                        Set<String> communityList = p._2()._2().get().getAccumulator();
                        for(Context c: r.getContext()){
                            if (communityList.contains(c.getId())){
                                //verify if the datainfo for this context contains propagation
                                if (!c.getDataInfo().stream().map(di -> di.getInferenceprovenance()).collect(Collectors.toSet()).contains(PROPAGATION_DATA_INFO_TYPE)){
                                    c.getDataInfo().add(getDataInfo(PROPAGATION_DATA_INFO_TYPE, PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_ID, PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_NAME));
                                    //community id already in the context of the result. Remove it from the set that has to be added
                                communityList.remove(c.getId());
                                }
                            }
                        }
                        List<Context> cc = r.getContext();
                        for(String cId: communityList){
                            Context context = new Context();
                            context.setId(cId);
                            context.setDataInfo(Arrays.asList(getDataInfo(PROPAGATION_DATA_INFO_TYPE, PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_ID, PROPAGATION_RESULT_COMMUNITY_SEMREL_CLASS_NAME)));
                            cc.add(context);
                        }
                        r.setContext(cc);
                    }
                    return r;
                })
                .map(p -> new ObjectMapper().writeValueAsString(p))
                .saveAsTextFile(outputPath+"/"+type);
    }



    private static TypedRow getTypedRow(List<String> communityIdList, List<Context> context, String id, String type) {
        Set<String> result_communities = context
                .stream()
                .map(c -> c.getId())
                .collect(Collectors.toSet());
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
