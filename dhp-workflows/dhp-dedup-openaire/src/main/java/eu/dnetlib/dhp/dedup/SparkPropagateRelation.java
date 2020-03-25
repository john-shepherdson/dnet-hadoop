package eu.dnetlib.dhp.dedup;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.pace.util.MapDocumentUtil;
import org.apache.commons.io.IOUtils;
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

public class SparkPropagateRelation {

    enum FieldType {
        SOURCE,
        TARGET
    }

    final static String SOURCEJSONPATH = "$.source";
    final static String TARGETJSONPATH = "$.target";

    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkPropagateRelation.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/propagateRelation_parameters.json")));
        parser.parseArgument(args);

        new SparkPropagateRelation().run(parser);
    }

    public void run(ArgumentApplicationParser parser) {

        final String graphBasePath = parser.get("graphBasePath");
        final String workingPath = parser.get("workingPath");
        final String dedupGraphPath = parser.get("dedupGraphPath");

        try (SparkSession spark = getSparkSession(parser)) {
            final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

            final Dataset<Relation> mergeRels = spark.read().load(DedupUtility.createMergeRelPath(workingPath, "*", "*")).as(Encoders.bean(Relation.class));

            final JavaPairRDD<String, String> mergedIds = mergeRels
                    .where("relClass == 'merges'")
                    .select(mergeRels.col("source"), mergeRels.col("target"))
                    .distinct()
                    .toJavaRDD()
                    .mapToPair((PairFunction<Row, String, String>) r -> new Tuple2<>(r.getString(1), r.getString(0)));

            JavaRDD<String> relations = sc.textFile(DedupUtility.createEntityPath(graphBasePath, "relation"));

            JavaRDD<String> newRels = relations.mapToPair(
                    (PairFunction<String, String, String>) s ->
                            new Tuple2<>(MapDocumentUtil.getJPathString(SOURCEJSONPATH, s), s))
                    .leftOuterJoin(mergedIds)
                    .map((Function<Tuple2<String, Tuple2<String, Optional<String>>>, String>) v1 -> {
                        if (v1._2()._2().isPresent()) {
                            return replaceField(v1._2()._1(), v1._2()._2().get(), FieldType.SOURCE);
                        }
                        return v1._2()._1();
                    })
                    .mapToPair(
                            (PairFunction<String, String, String>) s ->
                                    new Tuple2<>(MapDocumentUtil.getJPathString(TARGETJSONPATH, s), s))
                    .leftOuterJoin(mergedIds)
                    .map((Function<Tuple2<String, Tuple2<String, Optional<String>>>, String>) v1 -> {
                        if (v1._2()._2().isPresent()) {
                            return replaceField(v1._2()._1(), v1._2()._2().get(), FieldType.TARGET);
                        }
                        return v1._2()._1();
                    }).filter(SparkPropagateRelation::containsDedup)
                    .repartition(500);

            //update deleted by inference
            relations = relations.mapToPair(
                    (PairFunction<String, String, String>) s ->
                            new Tuple2<>(MapDocumentUtil.getJPathString(SOURCEJSONPATH, s), s))
                    .leftOuterJoin(mergedIds)
                    .map((Function<Tuple2<String, Tuple2<String, Optional<String>>>, String>) v1 -> {
                        if (v1._2()._2().isPresent()) {
                            return updateDeletedByInference(v1._2()._1(), Relation.class);
                        }
                        return v1._2()._1();
                    })
                    .mapToPair(
                            (PairFunction<String, String, String>) s ->
                                    new Tuple2<>(MapDocumentUtil.getJPathString(TARGETJSONPATH, s), s))
                    .leftOuterJoin(mergedIds)
                    .map((Function<Tuple2<String, Tuple2<String, Optional<String>>>, String>) v1 -> {
                        if (v1._2()._2().isPresent()) {
                            return updateDeletedByInference(v1._2()._1(), Relation.class);
                        }
                        return v1._2()._1();
                    })
                    .repartition(500);

            newRels.union(relations).repartition(1000)
                    .saveAsTextFile(DedupUtility.createEntityPath(dedupGraphPath, "relation"), GzipCodec.class);
        }
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

    private static SparkSession getSparkSession(ArgumentApplicationParser parser) {
        SparkConf conf = new SparkConf();

        return SparkSession
                .builder()
                .appName(SparkPropagateRelation.class.getSimpleName())
                .master(parser.get("master"))
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();
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
