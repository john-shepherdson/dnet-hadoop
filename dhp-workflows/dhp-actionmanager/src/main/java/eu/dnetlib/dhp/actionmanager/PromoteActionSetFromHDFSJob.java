package eu.dnetlib.dhp.actionmanager;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.apache.spark.sql.functions.*;

public class PromoteActionSetFromHDFSJob {

    public static void main(String[] args) throws Exception {
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(IOUtils.toString(
                PromoteActionSetFromHDFSJob.class
                        .getResourceAsStream("/eu/dnetlib/dhp/actionmanager/actionmanager_input_parameters.json")));
        parser.parseArgument(args);

        Boolean isSparkSessionManaged = Optional
                .ofNullable(parser.get("isSparkSessionManaged"))
                .map(Boolean::valueOf)
                .orElse(Boolean.TRUE);
        String inputGraphPath = parser.get("inputGraphPath");
        String inputActionSetPaths = parser.get("inputActionSetPaths");
        String graphTableName = parser.get("graphTableName");
        String outputGraphPath = parser.get("outputGraphPath");
        OafMergeAndGet.Strategy strategy = OafMergeAndGet.Strategy.valueOf(parser.get("mergeAndGetStrategy"));

        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(new Class[]{
                Author.class,
                Context.class,
                Country.class,
                DataInfo.class,
                eu.dnetlib.dhp.schema.oaf.Dataset.class,
                Datasource.class,
                ExternalReference.class,
                ExtraInfo.class,
                Field.class,
                GeoLocation.class,
                Instance.class,
                Journal.class,
                KeyValue.class,
                Oaf.class,
                OafEntity.class,
                OAIProvenance.class,
                Organization.class,
                OriginDescription.class,
                OtherResearchProduct.class,
                Project.class,
                Publication.class,
                Qualifier.class,
                Relation.class,
                Result.class,
                Software.class,
                StructuredProperty.class
        });

        SparkSession spark = null;
        try {
            spark = SparkSession.builder().config(conf).getOrCreate();

            //TODO make graph table generic using enums
            switch (graphTableName) {
                case "dataset":
                    processWith(spark,
                            String.format("%s/%s", inputGraphPath, graphTableName),
                            inputActionSetPaths,
                            outputGraphPath,
                            graphTableName,
                            strategy,
                            eu.dnetlib.dhp.schema.oaf.Dataset.class);
                    break;
                case "datasource":
                    processWith(spark,
                            String.format("%s/%s", inputGraphPath, graphTableName),
                            inputActionSetPaths,
                            outputGraphPath,
                            graphTableName,
                            strategy,
                            Datasource.class);
                    break;
                case "organization":
                    processWith(spark,
                            String.format("%s/%s", inputGraphPath, graphTableName),
                            inputActionSetPaths,
                            outputGraphPath,
                            graphTableName,
                            strategy,
                            Organization.class);
                    break;
                case "otherresearchproduct":
                    processWith(spark,
                            String.format("%s/%s", inputGraphPath, graphTableName),
                            inputActionSetPaths,
                            outputGraphPath,
                            graphTableName,
                            strategy,
                            OtherResearchProduct.class);
                    break;
                case "project":
                    processWith(spark,
                            String.format("%s/%s", inputGraphPath, graphTableName),
                            inputActionSetPaths,
                            outputGraphPath,
                            graphTableName,
                            strategy,
                            Project.class);
                    break;
                case "publication":
                    processWith(spark,
                            String.format("%s/%s", inputGraphPath, graphTableName),
                            inputActionSetPaths,
                            outputGraphPath,
                            graphTableName,
                            strategy,
                            Publication.class);
                    break;
                case "relation":
                    processWith(spark,
                            String.format("%s/%s", inputGraphPath, graphTableName),
                            inputActionSetPaths,
                            outputGraphPath,
                            graphTableName,
                            strategy,
                            Relation.class);
                    break;
                case "software":
                    processWith(spark,
                            String.format("%s/%s", inputGraphPath, graphTableName),
                            inputActionSetPaths,
                            outputGraphPath,
                            graphTableName,
                            strategy,
                            Software.class);
                    break;
                default:
                    throw new RuntimeException("error processing table: " + graphTableName);
            }
        } finally {
            if (Objects.nonNull(spark) && isSparkSessionManaged) {
                spark.stop();
            }
        }
    }

    private static final StructType KV_SCHEMA = StructType$.MODULE$.apply(
            Arrays.asList(
                    StructField$.MODULE$.apply("key", DataTypes.StringType, false, Metadata.empty()),
                    StructField$.MODULE$.apply("value", DataTypes.StringType, false, Metadata.empty())
            ));

    private static <T> Dataset<T> readGraphTable(SparkSession spark, String path, Class<T> clazz) {
        JavaRDD<Row> rows = JavaSparkContext
                .fromSparkContext(spark.sparkContext())
                .sequenceFile(path, Text.class, Text.class)
                .map(x -> RowFactory.create(x._1().toString(), x._2().toString()));

        return spark.createDataFrame(rows, KV_SCHEMA)
                .map((MapFunction<Row, T>) row -> new ObjectMapper().readValue(row.<String>getAs("value"), clazz),
                        Encoders.kryo(clazz));
    }

    private static Dataset<String> readActionSetPayloads(SparkSession spark, String inputActionSetPaths) {
        return Arrays
                .stream(inputActionSetPaths.split(","))
                .map(inputActionSetPath -> readActionSetPayload(spark, inputActionSetPath))
                .reduce(Dataset::union)
                .get();
    }

    private static Dataset<String> readActionSetPayload(SparkSession spark, String inputActionSetPath) {
        JavaRDD<Row> actionsRDD = JavaSparkContext
                .fromSparkContext(spark.sparkContext())
                .sequenceFile(inputActionSetPath, Text.class, Text.class)
                .map(x -> RowFactory.create(x._1().toString(), x._2().toString()));

        return spark.createDataFrame(actionsRDD, KV_SCHEMA)
                .select(unbase64(get_json_object(col("value"), "$.TargetValue"))
                        .cast(DataTypes.StringType).as("target_value_json"))
                .as(Encoders.STRING());
    }

    private static <T extends Oaf> void processWith(SparkSession spark,
                                                    String inputGraphTablePath,
                                                    String inputActionSetPaths,
                                                    String outputGraphPath,
                                                    String graphTableName,
                                                    OafMergeAndGet.Strategy strategy,
                                                    Class<T> clazz) {
//        System.out.println("===== tableDS =====");
        Dataset<T> tableDS = readGraphTable(spark, inputGraphTablePath, clazz)
                .cache();
//        tableDS.printSchema();
//        tableDS.show();
//        tableDS.explain();
//        System.out.println("DEBUG: tableDS.partitions=" + tableDS.rdd().getNumPartitions());

//        System.out.println("===== actionPayloadDS =====");
        Dataset<String> actionPayloadDS = readActionSetPayloads(spark, inputActionSetPaths)
                .cache();
//            actionPayloadDS.printSchema();
//            actionPayloadDS.show();
//            actionPayloadDS.explain();
//            System.out.println("DEBUG: actionPayloadDS.partitions=" + actionPayloadDS.rdd().getNumPartitions());

//        System.out.println("===== processed =====");
        Dataset<T> processed = processGraphTable(tableDS, actionPayloadDS, strategy, clazz);
//            processed.printSchema();
//            processed.show();
//            processed.explain();
//            System.out.println("DEBUG: processed.partitions=" + processed.rdd().getNumPartitions());

//        System.out.println("===== result =====");
        Dataset<T> result = processed
                .map((MapFunction<T, T>) value -> value, Encoders.bean(clazz));
//        result.printSchema();
//        result.show();
//        result.explain();
//        System.out.println("DEBUG: result.partitions=" + result.rdd().getNumPartitions());

        String outputGraphTablePath = String.format("%s/%s", outputGraphPath, graphTableName);
        result.write()
                .format("parquet")
                .save(outputGraphTablePath);
    }

    private static <T extends Oaf> Dataset<T> processGraphTable(Dataset<T> tableDS,
                                                                Dataset<String> actionPayloadDS,
                                                                OafMergeAndGet.Strategy strategy,
                                                                Class<T> clazz) {
        SerializableSupplier<Function<T, String>> oafIdFn = () -> x -> {
            if (x instanceof Relation) {
                Relation r = (Relation) x;
                return Optional.ofNullable(r.getSource())
                        .map(source -> Optional.ofNullable(r.getTarget())
                                .map(target -> Optional.ofNullable(r.getRelType())
                                        .map(relType -> Optional.ofNullable(r.getSubRelType())
                                                .map(subRelType -> Optional.ofNullable(r.getRelClass())
                                                        .map(relClass -> String.join(source, target, relType, subRelType, relClass))
                                                        .orElse(String.join(source, target, relType, subRelType))
                                                )
                                                .orElse(String.join(source, target, relType))
                                        )
                                        .orElse(String.join(source, target))
                                )
                                .orElse(source)
                        )
                        .orElse(null);
            }
            return ((OafEntity) x).getId();
        };

        SerializableSupplier<BiFunction<T, T, T>> mergeAndGetFn = OafMergeAndGet.functionFor(strategy);

        Dataset<T> joinedAndMerged = PromoteActionSetFromHDFSFunctions
                .joinOafEntityWithActionPayloadAndMerge(
                        tableDS,
                        actionPayloadDS,
                        oafIdFn,
                        () -> (json, c) -> {
                            try {
                                return new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).readValue(json, clazz);
                            } catch (IOException e) {
                                return null;
                            }
                        },
                        mergeAndGetFn,
                        clazz);

        return PromoteActionSetFromHDFSFunctions
                .groupOafByIdAndMerge(
                        joinedAndMerged,
                        oafIdFn,
                        mergeAndGetFn,
                        clazz
                );
    }
}
