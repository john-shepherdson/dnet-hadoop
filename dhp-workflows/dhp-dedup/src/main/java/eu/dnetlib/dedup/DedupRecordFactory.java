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
        long ts = System.currentTimeMillis();
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

        JavaPairRDD<String, Iterable<String>> sortedJoinResult = joinResult.groupByKey();

        switch (entityType) {
            case publication:
                return sortedJoinResult.map(p -> DedupRecordFactory.publicationMerger(p, ts));
            case dataset:
                return sortedJoinResult.map(d -> DedupRecordFactory.datasetMerger(d, ts));
            case project:
                return sortedJoinResult.map(p -> DedupRecordFactory.projectMerger(p, ts));
            case software:
                return sortedJoinResult.map(s -> DedupRecordFactory.softwareMerger(s, ts));
            case datasource:
                return sortedJoinResult.map(d -> DedupRecordFactory.datasourceMerger(d, ts));
            case organization:
                return sortedJoinResult.map(o -> DedupRecordFactory.organizationMerger(o, ts));
            case otherresearchproduct:
                return sortedJoinResult.map(o -> DedupRecordFactory.otherresearchproductMerger(o, ts));
            default:
                return null;
        }

    }

    private static Publication publicationMerger(Tuple2<String, Iterable<String>> e, final long ts) {

        Publication p = new Publication(); //the result of the merge, to be returned at the end

        p.setId(e._1());

        final ObjectMapper mapper = new ObjectMapper();

        final Collection<String> dateofacceptance = Lists.newArrayList();

        if (e._2() != null)
            e._2().forEach(pub -> {
                try {
                    Publication publication = mapper.readValue(pub, Publication.class);

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
        if (p.getDataInfo() == null)
            p.setDataInfo(new DataInfo());
        p.getDataInfo().setTrust("0.9");
        p.setLastupdatetimestamp(ts);
        return p;
    }

    private static Dataset datasetMerger(Tuple2<String, Iterable<String>> e, final long ts) {

        Dataset d = new Dataset(); //the result of the merge, to be returned at the end

        d.setId(e._1());

        final ObjectMapper mapper = new ObjectMapper();

        final Collection<String> dateofacceptance = Lists.newArrayList();

        if (e._2() != null)
            e._2().forEach(dat -> {
                try {
                    Dataset dataset = mapper.readValue(dat, Dataset.class);

                    d.mergeFrom(dataset);
                    d.setAuthor(DedupUtility.mergeAuthor(d.getAuthor(), dataset.getAuthor()));
                    //add to the list if they are not null
                    if (dataset.getDateofacceptance() != null)
                        dateofacceptance.add(dataset.getDateofacceptance().getValue());
                } catch (Exception exc) {
                    throw new RuntimeException(exc);
                }
            });
        d.setDateofacceptance(DatePicker.pick(dateofacceptance));
        if (d.getDataInfo() == null)
            d.setDataInfo(new DataInfo());
        d.getDataInfo().setTrust("0.9");
        d.setLastupdatetimestamp(ts);
        return d;
    }

    private static Project projectMerger(Tuple2<String, Iterable<String>> e, final long ts) {

        Project p = new Project(); //the result of the merge, to be returned at the end

        p.setId(e._1());

        final ObjectMapper mapper = new ObjectMapper();
        if (e._2() != null)
            e._2().forEach(proj -> {
                try {
                    Project project = mapper.readValue(proj, Project.class);

                    p.mergeFrom(project);
                } catch (Exception exc) {
                    throw new RuntimeException(exc);
                }
            });
        if (p.getDataInfo() == null)
            p.setDataInfo(new DataInfo());
        p.getDataInfo().setTrust("0.9");
        p.setLastupdatetimestamp(ts);
        return p;
    }

    private static Software softwareMerger(Tuple2<String, Iterable<String>> e, final long ts) {

        Software s = new Software(); //the result of the merge, to be returned at the end

        s.setId(e._1());
        final ObjectMapper mapper = new ObjectMapper();
        final Collection<String> dateofacceptance = Lists.newArrayList();
        if (e._2() != null)
            e._2().forEach(soft -> {
                try {
                    Software software = mapper.readValue(soft, Software.class);

                    s.mergeFrom(software);
                    s.setAuthor(DedupUtility.mergeAuthor(s.getAuthor(), software.getAuthor()));
                    //add to the list if they are not null
                    if (software.getDateofacceptance() != null)
                        dateofacceptance.add(software.getDateofacceptance().getValue());
                } catch (Exception exc) {
                    throw new RuntimeException(exc);
                }
            });
        s.setDateofacceptance(DatePicker.pick(dateofacceptance));
        if (s.getDataInfo() == null)
            s.setDataInfo(new DataInfo());
        s.getDataInfo().setTrust("0.9");
        s.setLastupdatetimestamp(ts);
        return s;
    }

    private static Datasource datasourceMerger(Tuple2<String, Iterable<String>> e, final long ts) {
        Datasource d = new Datasource(); //the result of the merge, to be returned at the end
        d.setId(e._1());
        final ObjectMapper mapper = new ObjectMapper();
        if (e._2() != null)
            e._2().forEach(dat -> {
                try {
                    Datasource datasource = mapper.readValue(dat, Datasource.class);

                    d.mergeFrom(datasource);
                } catch (Exception exc) {
                    throw new RuntimeException(exc);
                }
            });
        if (d.getDataInfo() == null)
            d.setDataInfo(new DataInfo());
        d.getDataInfo().setTrust("0.9");
        d.setLastupdatetimestamp(ts);
        return d;
    }

    private static Organization organizationMerger(Tuple2<String, Iterable<String>> e, final long ts) {

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

        if (o.getDataInfo() == null)
        {
            o.setDataInfo(new DataInfo());
        }
        if (o.getDataInfo() == null)
            o.setDataInfo(new DataInfo());
        o.getDataInfo().setTrust("0.9");
        o.setLastupdatetimestamp(ts);

        return o;
    }

    private static OtherResearchProduct otherresearchproductMerger(Tuple2<String, Iterable<String>> e, final long ts) {

        OtherResearchProduct o = new OtherResearchProduct(); //the result of the merge, to be returned at the end

        o.setId(e._1());

        final ObjectMapper mapper = new ObjectMapper();

        final Collection<String> dateofacceptance = Lists.newArrayList();

        if (e._2() != null)
            e._2().forEach(orp -> {
                try {
                    OtherResearchProduct otherResearchProduct = mapper.readValue(orp, OtherResearchProduct.class);

                    o.mergeFrom(otherResearchProduct);
                    o.setAuthor(DedupUtility.mergeAuthor(o.getAuthor(), otherResearchProduct.getAuthor()));
                    //add to the list if they are not null
                    if (otherResearchProduct.getDateofacceptance() != null)
                        dateofacceptance.add(otherResearchProduct.getDateofacceptance().getValue());
                } catch (Exception exc) {
                    throw new RuntimeException(exc);
                }
            });
        if (o.getDataInfo() == null)
            o.setDataInfo(new DataInfo());
        o.setDateofacceptance(DatePicker.pick(dateofacceptance));
        o.getDataInfo().setTrust("0.9");
        o.setLastupdatetimestamp(ts);
        return o;
    }

}
