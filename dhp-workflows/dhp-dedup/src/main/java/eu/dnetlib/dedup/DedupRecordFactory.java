package eu.dnetlib.dedup;

import com.google.common.collect.Lists;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.util.MapDocumentUtil;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jackson.map.ObjectMapper;
import scala.Tuple2;

import java.util.Collection;
import java.util.Random;

import static java.util.stream.Collectors.toMap;

public class DedupRecordFactory {

    public static JavaRDD<OafEntity> createDedupRecord(final JavaSparkContext sc, final SparkSession spark, final String mergeRelsInputPath, final String entitiesInputPath, final OafEntityType entityType, final DedupConfig dedupConf) {

        //<id, json_entity>
        final JavaPairRDD<String, String> inputJsonEntities = sc.textFile(entitiesInputPath)
                .mapToPair((PairFunction<String, String, String>) it ->
                        new Tuple2<String, String>(MapDocumentUtil.getJPathString(dedupConf.getWf().getIdPath(), it), it)
                );

        //<source, target>: source is the dedup_id, target is the id of the mergedIn
        JavaPairRDD<String, String> mergeRels = spark
                .read().load(mergeRelsInputPath).as(Encoders.bean(Relation.class))
                .where("relClass=='merges'")
                .javaRDD()
                .mapToPair(
                        (PairFunction<Relation, String, String>) r ->
                                new Tuple2<String, String>(r.getTarget(), r.getSource())
                );

        //<dedup_id, json_entity_merged>
        final JavaPairRDD<String, String> joinResult = mergeRels.join(inputJsonEntities).mapToPair((PairFunction<Tuple2<String, Tuple2<String, String>>, String, String>) Tuple2::_2);

        JavaPairRDD<OafKey, String> keyJson = joinResult.mapToPair((PairFunction<Tuple2<String, String>, OafKey, String>) json -> {

            String idValue = json._1();

            String trust = "";
            try {
                trust = MapDocumentUtil.getJPathString("$.dataInfo.trust", json._2());
            } catch (Throwable e) {

            }

            //TODO remember to replace this with the actual trust retrieving
            if (StringUtils.isBlank(trust)) {
                Random generator = new Random();
                int number = generator.nextInt(20);
                double result = (number / 100.0) + 0.80;
                trust = "" + result;
            }

            return new Tuple2<OafKey, String>(new OafKey(idValue, trust), json._2());
        });

        OafComparator c = new OafComparator();
        //<dedup_id, mergedRecordsSortedByTrust>
        JavaPairRDD<String, Iterable<String>> sortedJoinResult = keyJson.repartitionAndSortWithinPartitions(new OafPartitioner(keyJson.getNumPartitions()), c)
                .mapToPair((PairFunction<Tuple2<OafKey, String>, String, String>) t -> new Tuple2<String, String>(t._1().getDedupId(), t._2()))
                .groupByKey();


        switch (entityType) {
            case publication:
                return sortedJoinResult.map(DedupRecordFactory::publicationMerger);
            case dataset:
                return sortedJoinResult.map(DedupRecordFactory::datasetMerger);
            case project:
                return sortedJoinResult.map(DedupRecordFactory::projectMerger);
            case software:
                return sortedJoinResult.map(DedupRecordFactory::softwareMerger);
            case datasource:
                return sortedJoinResult.map(DedupRecordFactory::datasourceMerger);
            case organization:
                return sortedJoinResult.map(DedupRecordFactory::organizationMerger);
            case otherresearchproduct:
                return sortedJoinResult.map(DedupRecordFactory::otherresearchproductMerger);
            default:
                return null;
        }

    }

    private static Publication publicationMerger(Tuple2<String, Iterable<String>> e) {

        Publication p = new Publication(); //the result of the merge, to be returned at the end

        p.setId(e._1());

        final ObjectMapper mapper = new ObjectMapper();

        final Collection<String> dateofacceptance = Lists.newArrayList();


        StringBuilder trust = new StringBuilder("0.0");

        if (e._2() != null)
            e._2().forEach(pub -> {
                try {
                    Publication publication = mapper.readValue(pub, Publication.class);

                    final String currentTrust = publication.getDataInfo().getTrust();
                    if (!"1.0".equals(currentTrust)) {
                        trust.setLength(0);
                        trust.append(currentTrust);
                    }
                    p.mergeFrom(publication);
                    p.setAuthor(DedupUtility.mergeAuthor(p.getAuthor(), publication.getAuthor()));
                    //add to the list if they are not null
                    if (publication.getDateofacceptance() != null)
                        dateofacceptance.add(publication.getDateofacceptance().getValue());
                } catch (Exception exc) {
                    throw new RuntimeException(exc);
                }
            });
        p.setDateofacceptance(DatePicker.pick(dateofacceptance));
        return p;
    }

    private static Dataset datasetMerger(Tuple2<String, Iterable<String>> e) {

        throw new NotImplementedException();
    }

    private static Project projectMerger(Tuple2<String, Iterable<String>> e) {

        throw new NotImplementedException();
    }

    private static Software softwareMerger(Tuple2<String, Iterable<String>> e) {

        throw new NotImplementedException();
    }

    private static Datasource datasourceMerger(Tuple2<String, Iterable<String>> e) {

        throw new NotImplementedException();
    }

    private static Organization organizationMerger(Tuple2<String, Iterable<String>> e) {

        Organization o = new Organization(); //the result of the merge, to be returned at the end

        o.setId(e._1());

        final ObjectMapper mapper = new ObjectMapper();


        StringBuilder trust = new StringBuilder("0.0");

        if (e._2() != null)
            e._2().forEach(pub -> {
                try {
                    Organization organization = mapper.readValue(pub, Organization.class);

                    final String currentTrust = organization.getDataInfo().getTrust();
                    if (!"1.0".equals(currentTrust)) {
                        trust.setLength(0);
                        trust.append(currentTrust);
                    }
                    o.mergeFrom(organization);

                } catch (Exception exc) {
                    throw new RuntimeException(exc);
                }
            });

        return o;
    }

    private static OtherResearchProduct otherresearchproductMerger(Tuple2<String, Iterable<String>> e) {

        throw new NotImplementedException();
    }

}
