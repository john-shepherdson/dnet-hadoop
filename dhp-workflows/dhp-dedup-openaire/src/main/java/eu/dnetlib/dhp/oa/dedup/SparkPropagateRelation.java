package eu.dnetlib.dhp.oa.dedup;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import eu.dnetlib.pace.util.MapDocumentUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;

public class SparkPropagateRelation extends AbstractSparkAction {

    enum FieldType {
        SOURCE,
        TARGET
    }

    final static String SOURCEJSONPATH = "$.source";
    final static String TARGETJSONPATH = "$.target";

    private static final Log log = LogFactory.getLog(SparkPropagateRelation.class);

    public SparkPropagateRelation(ArgumentApplicationParser parser, SparkSession spark) throws Exception {
        super(parser, spark);
    }

    public static void main(String[] args) throws Exception {
        ArgumentApplicationParser parser = new ArgumentApplicationParser(
                IOUtils.toString(
                        SparkCreateSimRels.class.getResourceAsStream("/eu/dnetlib/dhp/oa/dedup/propagateRelation_parameters.json")));
        parser.parseArgument(args);

        new SparkPropagateRelation(parser, getSparkSession(parser)).run(ISLookupClientFactory.getLookUpService(parser.get("isLookUpUrl")));
    }

    @Override
    public void run(ISLookUpService isLookUpService) {

        final String graphBasePath = parser.get("graphBasePath");
        final String workingPath = parser.get("workingPath");
        final String dedupGraphPath = parser.get("dedupGraphPath");

        System.out.println(String.format("graphBasePath: '%s'", graphBasePath));
        System.out.println(String.format("workingPath:   '%s'", workingPath));
        System.out.println(String.format("dedupGraphPath:'%s'", dedupGraphPath));

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        final Dataset<Relation> mergeRels = spark.read().load(DedupUtility.createMergeRelPath(workingPath, "*", "*")).as(Encoders.bean(Relation.class));

        final JavaPairRDD<String, String> mergedIds = mergeRels
                .where("relClass == 'merges'")
                .select(mergeRels.col("source"), mergeRels.col("target"))
                .distinct()
                .toJavaRDD()
                .mapToPair((PairFunction<Row, String, String>) r -> new Tuple2<String, String>(r.getString(1), r.getString(0)));

        JavaRDD<String> relations = sc.textFile(DedupUtility.createEntityPath(graphBasePath, "relation"));

        relations.mapToPair(
                (PairFunction<String, String, String>) s ->
                        new Tuple2<String, String>(MapDocumentUtil.getJPathString(SOURCEJSONPATH, s), s))
                .leftOuterJoin(mergedIds)
                .map((Function<Tuple2<String, Tuple2<String, Optional<String>>>, String>) v1 -> {
                    if (v1._2()._2().isPresent()) {
                        return replaceField(v1._2()._1(), v1._2()._2().get(), FieldType.SOURCE);
                    }
                    return v1._2()._1();
                })
                .mapToPair(
                        (PairFunction<String, String, String>) s ->
                                new Tuple2<String, String>(MapDocumentUtil.getJPathString(TARGETJSONPATH, s), s))
                .leftOuterJoin(mergedIds)
                .map((Function<Tuple2<String, Tuple2<String, Optional<String>>>, String>) v1 -> {
                    if (v1._2()._2().isPresent()) {
                        return replaceField(v1._2()._1(), v1._2()._2().get(), FieldType.TARGET);
                    }
                    return v1._2()._1();
                }).filter(SparkPropagateRelation::containsDedup)
                .repartition(500)
                .saveAsTextFile(DedupUtility.createEntityPath(workingPath, "newRels"), GzipCodec.class);

        //update deleted by inference
        relations.mapToPair(
                (PairFunction<String, String, String>) s ->
                        new Tuple2<String, String>(MapDocumentUtil.getJPathString(SOURCEJSONPATH, s), s))
                .leftOuterJoin(mergedIds)
                .map((Function<Tuple2<String, Tuple2<String, Optional<String>>>, String>) v1 -> {
                    if (v1._2()._2().isPresent()) {
                        return updateDeletedByInference(v1._2()._1(), Relation.class);
                    }
                    return v1._2()._1();
                })
                .mapToPair(
                        (PairFunction<String, String, String>) s ->
                                new Tuple2<String, String>(MapDocumentUtil.getJPathString(TARGETJSONPATH, s), s))
                .leftOuterJoin(mergedIds)
                .map((Function<Tuple2<String, Tuple2<String, Optional<String>>>, String>) v1 -> {
                    if (v1._2()._2().isPresent()) {
                        return updateDeletedByInference(v1._2()._1(), Relation.class);
                    }
                    return v1._2()._1();
                })
                .repartition(500)
                .saveAsTextFile(DedupUtility.createEntityPath(workingPath, "updated"), GzipCodec.class);

        JavaRDD<String> newRels = sc
                .textFile(DedupUtility.createEntityPath(workingPath, "newRels"));

        sc
                .textFile(DedupUtility.createEntityPath(workingPath, "updated"))
                .union(newRels)
                .repartition(1000)
                .saveAsTextFile(DedupUtility.createEntityPath(dedupGraphPath, "relation"), GzipCodec.class);
    }

    private static boolean containsDedup(final String json) {
        final String source = MapDocumentUtil.getJPathString(SOURCEJSONPATH, json);
        final String target = MapDocumentUtil.getJPathString(TARGETJSONPATH, json);

        return source.toLowerCase().contains("dedup") || target.toLowerCase().contains("dedup");
    }

    private static String replaceField(final String json, final String id, final FieldType type) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            Relation relation = mapper.readValue(json, Relation.class);
            if (relation.getDataInfo() == null)
                relation.setDataInfo(new DataInfo());
            relation.getDataInfo().setDeletedbyinference(false);
            switch (type) {
                case SOURCE:
                    relation.setSource(id);
                    return mapper.writeValueAsString(relation);
                case TARGET:
                    relation.setTarget(id);
                    return mapper.writeValueAsString(relation);
                default:
                    throw new IllegalArgumentException("");
            }
        } catch (IOException e) {
            throw new RuntimeException("unable to deserialize json relation: " + json, e);
        }
    }

    private static <T extends Oaf> String updateDeletedByInference(final String json, final Class<T> clazz) {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            Oaf entity = mapper.readValue(json, clazz);
            if (entity.getDataInfo()== null)
                entity.setDataInfo(new DataInfo());
            entity.getDataInfo().setDeletedbyinference(true);
            return mapper.writeValueAsString(entity);
        } catch (IOException e) {
            throw new RuntimeException("Unable to convert json", e);
        }
    }
}