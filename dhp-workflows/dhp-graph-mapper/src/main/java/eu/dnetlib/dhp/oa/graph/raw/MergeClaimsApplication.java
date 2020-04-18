package eu.dnetlib.dhp.oa.graph.raw;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.dhp.schema.common.ModelSupport.isSubClass;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.HdfsSupport;
import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.*;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class MergeClaimsApplication {

    private static final Logger log = LoggerFactory.getLogger(MergeClaimsApplication.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(final String[] args) throws Exception {
        final ArgumentApplicationParser parser =
                new ArgumentApplicationParser(
                        IOUtils.toString(
                                MigrateMongoMdstoresApplication.class.getResourceAsStream(
                                        "/eu/dnetlib/dhp/oa/graph/merge_claims_parameters.json")));
        parser.parseArgument(args);

        Boolean isSparkSessionManaged =
                Optional.ofNullable(parser.get("isSparkSessionManaged"))
                        .map(Boolean::valueOf)
                        .orElse(Boolean.TRUE);
        log.info("isSparkSessionManaged: {}", isSparkSessionManaged);

        final String rawGraphPath = parser.get("rawGraphPath");
        log.info("rawGraphPath: {}", rawGraphPath);

        final String claimsGraphPath = parser.get("claimsGraphPath");
        log.info("claimsGraphPath: {}", claimsGraphPath);

        final String outputRawGaphPath = parser.get("outputRawGaphPath");
        log.info("outputRawGaphPath: {}", outputRawGaphPath);

        String graphTableClassName = parser.get("graphTableClassName");
        log.info("graphTableClassName: {}", graphTableClassName);

        Class<? extends Oaf> clazz = (Class<? extends Oaf>) Class.forName(graphTableClassName);

        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(ModelSupport.getOafModelClasses());

        runWithSparkSession(
                conf,
                isSparkSessionManaged,
                spark -> {
                    String type = clazz.getSimpleName().toLowerCase();

                    String rawPath = rawGraphPath + "/" + type;
                    String claimPath = claimsGraphPath + "/" + type;
                    String outPath = outputRawGaphPath + "/" + type;

                    removeOutputDir(spark, outPath);
                    mergeByType(spark, rawPath, claimPath, outPath, clazz);
                });
    }

    private static <T extends Oaf> void mergeByType(
            SparkSession spark, String rawPath, String claimPath, String outPath, Class<T> clazz) {
        Dataset<Tuple2<String, T>> raw =
                readFromPath(spark, rawPath, clazz)
                        .map(
                                (MapFunction<T, Tuple2<String, T>>)
                                        value -> new Tuple2<>(idFn().apply(value), value),
                                Encoders.tuple(Encoders.STRING(), Encoders.kryo(clazz)));

        final JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        Dataset<Tuple2<String, T>> claim =
                jsc.broadcast(readFromPath(spark, claimPath, clazz))
                        .getValue()
                        .map(
                                (MapFunction<T, Tuple2<String, T>>)
                                        value -> new Tuple2<>(idFn().apply(value), value),
                                Encoders.tuple(Encoders.STRING(), Encoders.kryo(clazz)));

        /*
        Dataset<Tuple2<String, T>> claim = readFromPath(spark, claimPath, clazz)
        		.map((MapFunction<T, Tuple2<String, T>>) value -> new Tuple2<>(idFn().apply(value), value), Encoders.tuple(Encoders.STRING(), Encoders.kryo(clazz)));
        */

        raw.joinWith(claim, raw.col("_1").equalTo(claim.col("_1")), "full_outer")
                .map(
                        (MapFunction<Tuple2<Tuple2<String, T>, Tuple2<String, T>>, T>)
                                value -> {
                                    Optional<Tuple2<String, T>> opRaw =
                                            Optional.ofNullable(value._1());
                                    Optional<Tuple2<String, T>> opClaim =
                                            Optional.ofNullable(value._2());

                                    return opRaw.isPresent()
                                            ? opRaw.get()._2()
                                            : opClaim.isPresent() ? opClaim.get()._2() : null;
                                },
                        Encoders.bean(clazz))
                .filter(Objects::nonNull)
                .map(
                        (MapFunction<T, String>) value -> OBJECT_MAPPER.writeValueAsString(value),
                        Encoders.STRING())
                .write()
                .mode(SaveMode.Overwrite)
                .option("compression", "gzip")
                .text(outPath);
    }

    private static <T extends Oaf> Dataset<T> readFromPath(
            SparkSession spark, String path, Class<T> clazz) {
        return spark.read()
                .textFile(path)
                .map(
                        (MapFunction<String, T>) value -> OBJECT_MAPPER.readValue(value, clazz),
                        Encoders.bean(clazz))
                .filter((FilterFunction<T>) value -> Objects.nonNull(idFn().apply(value)));
        /*
        return spark.read()
        		.load(path)
        		.as(Encoders.bean(clazz))
        		.filter((FilterFunction<T>) value -> Objects.nonNull(idFn().apply(value)));
         */
    }

    private static void removeOutputDir(SparkSession spark, String path) {
        HdfsSupport.remove(path, spark.sparkContext().hadoopConfiguration());
    }

    private static <T extends Oaf> Function<T, String> idFn() {
        return x -> {
            if (isSubClass(x, Relation.class)) {
                return idFnForRelation(x);
            }
            return idFnForOafEntity(x);
        };
    }

    private static <T extends Oaf> String idFnForRelation(T t) {
        Relation r = (Relation) t;
        return Optional.ofNullable(r.getSource())
                .map(
                        source ->
                                Optional.ofNullable(r.getTarget())
                                        .map(
                                                target ->
                                                        Optional.ofNullable(r.getRelType())
                                                                .map(
                                                                        relType ->
                                                                                Optional.ofNullable(
                                                                                                r
                                                                                                        .getSubRelType())
                                                                                        .map(
                                                                                                subRelType ->
                                                                                                        Optional
                                                                                                                .ofNullable(
                                                                                                                        r
                                                                                                                                .getRelClass())
                                                                                                                .map(
                                                                                                                        relClass ->
                                                                                                                                String
                                                                                                                                        .join(
                                                                                                                                                source,
                                                                                                                                                target,
                                                                                                                                                relType,
                                                                                                                                                subRelType,
                                                                                                                                                relClass))
                                                                                                                .orElse(
                                                                                                                        String
                                                                                                                                .join(
                                                                                                                                        source,
                                                                                                                                        target,
                                                                                                                                        relType,
                                                                                                                                        subRelType)))
                                                                                        .orElse(
                                                                                                String
                                                                                                        .join(
                                                                                                                source,
                                                                                                                target,
                                                                                                                relType)))
                                                                .orElse(
                                                                        String.join(
                                                                                source, target)))
                                        .orElse(source))
                .orElse(null);
    }

    private static <T extends Oaf> String idFnForOafEntity(T t) {
        return ((OafEntity) t).getId();
    }
}
