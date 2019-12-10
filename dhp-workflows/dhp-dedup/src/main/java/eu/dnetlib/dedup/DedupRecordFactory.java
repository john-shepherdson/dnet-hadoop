package eu.dnetlib.dedup;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.pace.config.DedupConfig;
import eu.dnetlib.pace.util.MapDocumentUtil;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jackson.map.ObjectMapper;
import scala.Tuple2;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import static java.util.stream.Collectors.toMap;

public class DedupRecordFactory {

    public JavaRDD<OafEntity> createDedupRecord(final JavaSparkContext sc, final SparkSession spark, final String mergeRelsInputPath, final String entitiesInputPath, final OafEntityType entityType, final DedupConfig dedupConf){

        //<id, json_entity>
        final JavaPairRDD<String, String> inputJsonEntities = sc.textFile(entitiesInputPath)
                .mapToPair((PairFunction<String,String,String>) it->
                        new Tuple2<String, String>(MapDocumentUtil.getJPathString(dedupConf.getWf().getIdPath(), it),it)
                );

        //<source, target>: source is the dedup_id, target is the id of the mergedIn
        JavaPairRDD<String,String> mergeRels = spark
                .read().load(mergeRelsInputPath).as(Encoders.bean(Relation.class))
                .where("relClass=='merges'")
                .javaRDD()
                .mapToPair(
                        (PairFunction<Relation, String,String>)r->
                                new Tuple2<String, String>(r.getTarget(), r.getSource())
                );

        //<dedup_id, json_entity_merged>
        final JavaPairRDD<String, String> joinResult = mergeRels.join(inputJsonEntities).mapToPair((PairFunction<Tuple2<String, Tuple2<String, String>>, String, String>) Tuple2::_2);

        JavaPairRDD<OafKey, String> keyJson = joinResult.mapToPair((PairFunction<Tuple2<String, String>, OafKey, String>)  json -> {

            String idValue = json._1();

            String trust = MapDocumentUtil.getJPathString("$.dataInfo.trust", json._2());

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


        switch(entityType){
            case Publication:
                return sortedJoinResult.map(this::publicationMerger);
            case Dataset:
                return sortedJoinResult.map(this::datasetMerger);
            case Project:
                return sortedJoinResult.map(this::projectMerger);
            case Software:
                return sortedJoinResult.map(this::softwareMerger);
            case Datasource:
                return sortedJoinResult.map(this::datasourceMerger);
            case Organization:
                return sortedJoinResult.map(this::organizationMerger);
            case OtherResearchProduct:
                return sortedJoinResult.map(this::otherresearchproductMerger);
            default:
                return null;
        }

    }

    private Publication publicationMerger(Tuple2<String, Iterable<String>> e){

        Publication p = new Publication(); //the result of the merge, to be returned at the end

        p.setId(e._1());

        final ObjectMapper mapper = new ObjectMapper();

        final Collection<String> dateofacceptance = Lists.newArrayList();
        final Collection<List<Author>> authors = Lists.newArrayList();
        final Collection<List<Instance>> instances = Lists.newArrayList();

        StringBuilder trust = new StringBuilder("0.0");

        e._2().forEach(pub -> {
            try {
                Publication publication = mapper.readValue(pub, Publication.class);

                final String currentTrust = publication.getDataInfo().getTrust();
                if (!currentTrust.equals("1.0")) {
                    trust.setLength(0);
                    trust.append(currentTrust);
                }

                p.mergeFrom(publication);

                //add to the list if they are not null
                if (publication.getDateofacceptance() != null)
                    dateofacceptance.add(publication.getDateofacceptance().getValue());
                if (publication.getAuthor() != null)
                    authors.add(publication.getAuthor());
                if (publication.getInstance() != null)
                    instances.add(publication.getInstance());

            } catch (Exception exc){}

        });

        p.setAuthor(null); //TODO create a single list of authors to put in the final publication


        return p;
    }

    private Dataset datasetMerger(Tuple2<String, Iterable<String>> e){

        throw new NotImplementedException();
    }

    private Project projectMerger(Tuple2<String, Iterable<String>> e){

        throw new NotImplementedException();
    }

    private Software softwareMerger(Tuple2<String, Iterable<String>> e){

        throw new NotImplementedException();
    }

    private Datasource datasourceMerger(Tuple2<String, Iterable<String>> e){

        throw new NotImplementedException();
    }

    private Organization organizationMerger(Tuple2<String, Iterable<String>> e){

        throw new NotImplementedException();
    }

    private OtherResearchProduct otherresearchproductMerger(Tuple2<String, Iterable<String>> e){

        throw new NotImplementedException();
    }

}
