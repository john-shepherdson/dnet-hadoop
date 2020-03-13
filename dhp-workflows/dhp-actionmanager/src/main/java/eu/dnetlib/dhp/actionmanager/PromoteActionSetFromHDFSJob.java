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
import org.apache.spark.api.java.function.FilterFunction;
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

    // TODO replace with project's common implementation
    public enum GraphTableName {
        DATASET, DATASOURCE, ORGANIZATION, OTHERRESEARCHPRODUCT, PROJECT, PUBLICATION, RELATION, SOFTWARE
    }

    private static final StructType KV_SCHEMA = StructType$.MODULE$.apply(
            Arrays.asList(
                    StructField$.MODULE$.apply("key", DataTypes.StringType, false, Metadata.empty()),
                    StructField$.MODULE$.apply("value", DataTypes.StringType, false, Metadata.empty())
            ));

    public static void main(String[] args) throws Exception {
        String jsonConfiguration = IOUtils.toString(
                PromoteActionSetFromHDFSJob.class
                        .getResourceAsStream("/eu/dnetlib/dhp/actionmanager/actionmanager_input_parameters.json"));
        final ArgumentApplicationParser parser = new ArgumentApplicationParser(jsonConfiguration);
        parser.parseArgument(args);

        Boolean isSparkSessionManaged = Optional
                .ofNullable(parser.get("isSparkSessionManaged"))
                .map(Boolean::valueOf)
                .orElse(Boolean.TRUE);
        String inputGraphPath = parser.get("inputGraphPath");
        String inputActionSetPaths = parser.get("inputActionSetPaths");
        GraphTableName graphTableName = GraphTableName.valueOf(parser.get("graphTableName").toUpperCase());
        String outputGraphPath = parser.get("outputGraphPath");
        OafMergeAndGet.Strategy strategy = OafMergeAndGet.Strategy.valueOf(parser.get("mergeAndGetStrategy").toUpperCase());

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
            String inputGraphTablePath = String.format("%s/%s", inputGraphPath, graphTableName.name().toLowerCase());
            String outputGraphTablePath = String.format("%s/%s", outputGraphPath, graphTableName.name().toLowerCase());

            switch (graphTableName) {
                case DATASET:
                    processGraphTable(spark,
                            inputGraphTablePath,
                            inputActionSetPaths,
                            outputGraphTablePath,
                            strategy,
                            eu.dnetlib.dhp.schema.oaf.Dataset.class);
                    break;
                case DATASOURCE:
                    processGraphTable(spark,
                            inputGraphTablePath,
                            inputActionSetPaths,
                            outputGraphTablePath,
                            strategy,
                            Datasource.class);
                    break;
                case ORGANIZATION:
                    processGraphTable(spark,
                            inputGraphTablePath,
                            inputActionSetPaths,
                            outputGraphTablePath,
                            strategy,
                            Organization.class);
                    break;
                case OTHERRESEARCHPRODUCT:
                    processGraphTable(spark,
                            inputGraphTablePath,
                            inputActionSetPaths,
                            outputGraphTablePath,
                            strategy,
                            OtherResearchProduct.class);
                    break;
                case PROJECT:
                    processGraphTable(spark,
                            inputGraphTablePath,
                            inputActionSetPaths,
                            outputGraphTablePath,
                            strategy,
                            Project.class);
                    break;
                case PUBLICATION:
                    processGraphTable(spark,
                            inputGraphTablePath,
                            inputActionSetPaths,
                            outputGraphTablePath,
                            strategy,
                            Publication.class);
                    break;
                case RELATION:
                    processGraphTable(spark,
                            inputGraphTablePath,
                            inputActionSetPaths,
                            outputGraphTablePath,
                            strategy,
                            Relation.class);
                    break;
                case SOFTWARE:
                    processGraphTable(spark,
                            inputGraphTablePath,
                            inputActionSetPaths,
                            outputGraphTablePath,
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

    private static <T extends Oaf> void processGraphTable(SparkSession spark,
                                                          String inputGraphTablePath,
                                                          String inputActionSetPaths,
                                                          String outputGraphTablePath,
                                                          OafMergeAndGet.Strategy strategy,
                                                          Class<T> clazz) {
        Dataset<T> tableDS = readGraphTable(spark, inputGraphTablePath, clazz)
                .cache();
        Dataset<T> actionPayloadDS = readActionSetPayloads(spark, inputActionSetPaths, clazz)
                .cache();

        Dataset<T> result = promoteActionSetForGraphTable(tableDS, actionPayloadDS, strategy, clazz)
                .map((MapFunction<T, T>) value -> value, Encoders.bean(clazz));

        saveGraphTableAsParquet(result, outputGraphTablePath);
    }

    private static <T extends Oaf> Dataset<T> readGraphTable(SparkSession spark,
                                                             String inputGraphTablePath,
                                                             Class<T> clazz) {
        JavaRDD<Row> rows = JavaSparkContext
                .fromSparkContext(spark.sparkContext())
                .sequenceFile(inputGraphTablePath, Text.class, Text.class)
                .map(x -> RowFactory.create(x._1().toString(), x._2().toString()));

        return spark.createDataFrame(rows, KV_SCHEMA)
                .map((MapFunction<Row, T>) row -> new ObjectMapper().readValue(row.<String>getAs("value"), clazz),
                        Encoders.bean(clazz));
    }

    private static <T extends Oaf> Dataset<T> readActionSetPayloads(SparkSession spark,
                                                                    String inputActionSetPaths,
                                                                    Class<T> clazz) {
        return Arrays
                .stream(inputActionSetPaths.split(","))
                .map(inputActionSetPath -> readActionSetPayload(spark, inputActionSetPath, clazz))
                .reduce(Dataset::union)
                .orElseThrow(() -> new RuntimeException("error reading action sets: " + inputActionSetPaths));
    }

    private static <T extends Oaf> Dataset<T> readActionSetPayload(SparkSession spark,
                                                                   String inputActionSetPath,
                                                                   Class<T> clazz) {
        JavaRDD<Row> actionsRDD = JavaSparkContext
                .fromSparkContext(spark.sparkContext())
                .sequenceFile(inputActionSetPath, Text.class, Text.class)
                .map(x -> RowFactory.create(x._1().toString(), x._2().toString()));

        SerializableSupplier<BiFunction<String, Class<T>, T>> actionPayloadToOafFn = () -> (json, c) -> {
            try {
                return new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).readValue(json, c);
            } catch (IOException e) {
                return null;
            }
        };

        return spark.createDataFrame(actionsRDD, KV_SCHEMA)
                .select(unbase64(get_json_object(col("value"), "$.TargetValue"))
                        .cast(DataTypes.StringType).as("target_value_json"))
                .as(Encoders.STRING())
                .map((MapFunction<String, T>) value -> actionPayloadToOafFn.get().apply(value, clazz), Encoders.bean(clazz))
                .filter((FilterFunction<T>) Objects::nonNull);
    }

    private static <T extends Oaf> Dataset<T> promoteActionSetForGraphTable(Dataset<T> tableDS,
                                                                            Dataset<T> actionPayloadDS,
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

    private static <T extends Oaf> void saveGraphTableAsParquet(Dataset<T> result, String outputGraphTablePath) {
        result.write()
                .format("parquet")
                .save(outputGraphTablePath);
    }
}
