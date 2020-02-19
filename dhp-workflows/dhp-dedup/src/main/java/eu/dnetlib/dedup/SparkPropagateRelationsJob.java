package eu.dnetlib.dedup;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.utils.DHPUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.GzipCodec;
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

public class SparkPropagateRelationsJob {
    enum FieldType {
        SOURCE,
        TARGET
    }
    final static String IDJSONPATH = "$.id";
    final static String SOURCEJSONPATH = "$.source";
    final static String TARGETJSONPATH = "$.target";

    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(SparkPropagateRelationsJob.class.getResourceAsStream("/eu/dnetlib/dhp/dedup/dedup_propagate_relation_parameters.json")));
        parser.parseArgument(args);
        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkUpdateEntityJob.class.getSimpleName())
                .master(parser.get("master"))
                .getOrCreate();

        final JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        final String relationPath = parser.get("relationPath");
        final String mergeRelPath = parser.get("mergeRelPath");
        final String targetRelPath = parser.get("targetRelPath");


        final Dataset<Relation> df = spark.read().load(mergeRelPath).as(Encoders.bean(Relation.class));



        final JavaPairRDD<String, String> mergedIds = df
                .where("relClass == 'merges'")
                .select(df.col("source"),df.col("target"))
                .distinct()
                .toJavaRDD()
                .mapToPair((PairFunction<Row, String, String>) r -> new Tuple2<>(r.getString(1), r.getString(0)));


        final JavaRDD<String> sourceEntity = sc.textFile(relationPath);
        JavaRDD<String> newRels = sourceEntity.mapToPair(
                (PairFunction<String, String, String>) s ->
                        new Tuple2<>(DHPUtils.getJPathString(SOURCEJSONPATH, s), s))
                .leftOuterJoin(mergedIds)
                .map((Function<Tuple2<String, Tuple2<String, Optional<String>>>, String>) v1 -> {
                    if (v1._2()._2().isPresent()) {
                        return replaceField(v1._2()._1(), v1._2()._2().get(), FieldType.SOURCE);
                    }
                    return v1._2()._1();
                })
                .mapToPair(
                        (PairFunction<String, String, String>) s ->
                                new Tuple2<>(DHPUtils.getJPathString(TARGETJSONPATH, s), s))
                .leftOuterJoin(mergedIds)
                .map((Function<Tuple2<String, Tuple2<String, Optional<String>>>, String>) v1 -> {
                    if (v1._2()._2().isPresent()) {
                        return replaceField(v1._2()._1(), v1._2()._2().get(), FieldType.TARGET);
                    }
                    return v1._2()._1();
                }).filter(SparkPropagateRelationsJob::containsDedup)
                .repartition(500);

        newRels.union(sourceEntity).repartition(1000).saveAsTextFile(targetRelPath, GzipCodec.class);
    }

    private static boolean containsDedup(final String json) {
        final String source = DHPUtils.getJPathString(SOURCEJSONPATH, json);
        final String target = DHPUtils.getJPathString(TARGETJSONPATH, json);

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
}
