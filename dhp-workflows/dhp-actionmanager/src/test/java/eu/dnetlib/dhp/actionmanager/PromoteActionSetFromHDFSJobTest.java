package eu.dnetlib.dhp.actionmanager;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.oaf.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.*;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.base64;
import static org.apache.spark.sql.functions.col;
import static org.junit.Assert.*;

public class PromoteActionSetFromHDFSJobTest {
    private static SparkSession spark;
    private static Configuration configuration;

    private Path workingDir;
    private Path inputDir;
    private Path inputGraphDir;
    private Path inputActionSetsDir;
    private Path outputDir;

    private ClassLoader cl = getClass().getClassLoader();

    @BeforeClass
    public static void beforeClass() throws IOException {
        SparkConf conf = new SparkConf();
        conf.setAppName(PromoteActionSetFromHDFSJobTest.class.getSimpleName());
        conf.setMaster("local");
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

        spark = SparkSession.builder().config(conf).getOrCreate();

        configuration = Job.getInstance().getConfiguration();
    }

    @Before
    public void before() throws IOException {
        workingDir = Files.createTempDirectory("promote_action_set");
        inputDir = workingDir.resolve("input");
        inputGraphDir = inputDir.resolve("graph");

        inputActionSetsDir = inputDir.resolve("action_sets");

        outputDir = workingDir.resolve("output");
    }

    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
    }

    @AfterClass
    public static void afterAll() {
        spark.stop();
    }

    @Test
    public void shouldThrowWhenGraphTableIsInvalid() {
        fail("not yet implemented");
    }

    @Test
    public void shouldReadActionsFromHDFSAndPromoteThemForDatasetsUsingMergeFromStrategy() throws Exception {
        readActionsFromHDFSAndPromoteThemFor(PromoteActionSetFromHDFSJob.GraphTableName.DATASET,
                OafMergeAndGet.Strategy.MERGE_FROM_AND_GET,
                eu.dnetlib.dhp.schema.oaf.Dataset.class);
    }

    @Test
    public void shouldReadActionsFromHDFSAndPromoteThemForDatasourcesUsingMergeFromStrategy() throws Exception {
        readActionsFromHDFSAndPromoteThemFor(PromoteActionSetFromHDFSJob.GraphTableName.DATASOURCE,
                OafMergeAndGet.Strategy.MERGE_FROM_AND_GET,
                Datasource.class);
    }

    @Test
    public void shouldReadActionsFromHDFSAndPromoteThemForOrganizationsUsingMergeFromStrategy() throws Exception {
        readActionsFromHDFSAndPromoteThemFor(PromoteActionSetFromHDFSJob.GraphTableName.ORGANIZATION,
                OafMergeAndGet.Strategy.MERGE_FROM_AND_GET,
                Organization.class);
    }

    @Test
    public void shouldReadActionsFromHDFSAndPromoteThemForOtherResearchProductsUsingMergeFromStrategy() throws Exception {
        readActionsFromHDFSAndPromoteThemFor(PromoteActionSetFromHDFSJob.GraphTableName.OTHERRESEARCHPRODUCT,
                OafMergeAndGet.Strategy.MERGE_FROM_AND_GET,
                OtherResearchProduct.class);
    }

    @Test
    public void shouldReadActionsFromHDFSAndPromoteThemForProjectsUsingMergeFromStrategy() throws Exception {
        readActionsFromHDFSAndPromoteThemFor(PromoteActionSetFromHDFSJob.GraphTableName.PROJECT,
                OafMergeAndGet.Strategy.MERGE_FROM_AND_GET,
                Project.class);
    }

    @Test
    public void shouldReadActionsFromHDFSAndPromoteThemForPublicationsUsingMergeFromStrategy() throws Exception {
        readActionsFromHDFSAndPromoteThemFor(PromoteActionSetFromHDFSJob.GraphTableName.PUBLICATION,
                OafMergeAndGet.Strategy.MERGE_FROM_AND_GET,
                Publication.class);
    }

    @Test
    public void shouldReadActionsFromHDFSAndPromoteThemForRelationsUsingMergeFromStrategy() throws Exception {
        readActionsFromHDFSAndPromoteThemFor(PromoteActionSetFromHDFSJob.GraphTableName.RELATION,
                OafMergeAndGet.Strategy.MERGE_FROM_AND_GET,
                Relation.class);
    }

    @Test
    public void shouldReadActionsFromHDFSAndPromoteThemForSoftwaresUsingMergeFromStrategy() throws Exception {
        readActionsFromHDFSAndPromoteThemFor(PromoteActionSetFromHDFSJob.GraphTableName.SOFTWARE,
                OafMergeAndGet.Strategy.MERGE_FROM_AND_GET,
                Software.class);
    }

    private <T extends Oaf> void readActionsFromHDFSAndPromoteThemFor(PromoteActionSetFromHDFSJob.GraphTableName graphTableName,
                                                                      OafMergeAndGet.Strategy strategy,
                                                                      Class<T> clazz) throws Exception {
        // given
        String inputGraphTableJsonDumpPath =
                String.format("%s/%s.json", "eu/dnetlib/dhp/actionmanager/input/graph", graphTableName.name().toLowerCase());
        createGraphTableFor(inputGraphTableJsonDumpPath, graphTableName.name().toLowerCase(), clazz);
        String inputActionSetPaths = createActionSets();
        Path outputGraphDir = outputDir.resolve("graph");

        // when
        PromoteActionSetFromHDFSJob.main(new String[]{
                "-isSparkSessionManaged", Boolean.FALSE.toString(),
                "-inputGraphPath", inputGraphDir.toString(),
                "-inputActionSetPaths", inputActionSetPaths,
                "-graphTableName", graphTableName.name(),
                "-outputGraphPath", outputGraphDir.toString(),
                "-mergeAndGetStrategy", strategy.name()
        });

        // then
        Path outputGraphTableDir = outputGraphDir.resolve(graphTableName.name().toLowerCase());
        assertTrue(Files.exists(outputGraphDir));

        List<T> outputGraphTableRows = readGraphTableFromParquet(outputGraphTableDir.toString(), clazz).collectAsList();
        outputGraphTableRows.sort(Comparator.comparingInt(Object::hashCode));

        assertEquals(10, outputGraphTableRows.size());

        String expectedOutputGraphTableJsonDumpPath =
                String.format("%s/%s/%s.json", "eu/dnetlib/dhp/actionmanager/output/graph", strategy.name().toLowerCase(), graphTableName.name().toLowerCase());
        Path expectedOutputGraphTableJsonDumpFile = Paths
                .get(Objects.requireNonNull(cl.getResource(expectedOutputGraphTableJsonDumpPath)).getFile());
        List<T> expectedOutputGraphTableRows = readGraphTableFromJSON(
                expectedOutputGraphTableJsonDumpFile, clazz)
                .collectAsList();
        expectedOutputGraphTableRows.sort(Comparator.comparingInt(Object::hashCode));

        assertArrayEquals(expectedOutputGraphTableRows.toArray(), outputGraphTableRows.toArray());
    }

    private <T extends Oaf> void createGraphTableFor(String inputGraphTableJsonDumpPath,
                                                     String inputGraphTableDirRelativePath,
                                                     Class<T> clazz) {
        Path inputGraphTableJsonDumpFile = Paths
                .get(Objects.requireNonNull(cl.getResource(inputGraphTableJsonDumpPath)).getFile());
        Dataset<T> inputGraphTableDS = readGraphTableFromJSON(inputGraphTableJsonDumpFile, clazz);
        Path inputGraphTableDir = inputGraphDir.resolve(inputGraphTableDirRelativePath);
        inputGraphTableDS
                .toJSON()
                .javaRDD()
                .mapToPair(json -> new Tuple2<>(new Text(clazz.getCanonicalName()), new Text(json)))
                .saveAsNewAPIHadoopFile(inputGraphTableDir.toString(),
                        Text.class,
                        Text.class,
                        SequenceFileOutputFormat.class,
                        configuration);
    }

    private String createActionSets() throws IOException {
        Path inputActionPayloadJsonDumpsDir = Paths
                .get(Objects.requireNonNull(cl.getResource("eu/dnetlib/dhp/actionmanager/input/action_sets/"))
                        .getFile());
        Files
                .list(inputActionPayloadJsonDumpsDir)
                .forEach(inputActionPayloadJsonDump -> {
                    String inputActionSetId = inputActionPayloadJsonDump.getFileName().toString();
                    Path inputActionSetDir = inputActionSetsDir.resolve(inputActionPayloadJsonDump.getFileName());
                    spark.read()
                            .textFile(inputActionPayloadJsonDump.toString())
                            .withColumn("TargetValue", base64(col("value")))
                            .select("TargetValue")
                            .toJSON()
                            .javaRDD()
                            .mapToPair(json -> new Tuple2<>(new Text(inputActionSetId), new Text(json)))
                            .saveAsNewAPIHadoopFile(inputActionSetDir.toString(),
                                    Text.class,
                                    Text.class,
                                    SequenceFileOutputFormat.class,
                                    configuration);
                });
        return Files.list(inputActionSetsDir).map(Path::toString).collect(Collectors.joining(","));
    }

    private static <T extends Oaf> Dataset<T> readGraphTableFromJSON(Path path, Class<T> clazz) {
        return spark.read()
                .format("json")
                .load(path.toString())
                .toJSON()
                .map((MapFunction<String, T>) json -> newObjectMapper().readValue(json, clazz), Encoders.bean(clazz));
    }

    //TODO: enable strict deserialization later
    private static ObjectMapper newObjectMapper() {
        return new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    private static <T extends Oaf> Dataset<T> readGraphTableFromParquet(String outputGraphTablePath, Class<T> clazz) {
        return spark.read()
                .format("parquet")
                .load(outputGraphTablePath)
                .as(Encoders.bean(clazz));
    }
}