
package eu.dnetlib.dhp.aggregation;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import eu.dnetlib.dhp.collection.GenerateNativeStoreSparkJob;
import eu.dnetlib.dhp.common.vocabulary.VocabularyGroup;
import eu.dnetlib.dhp.transformation.TransformSparkJobNode;
import eu.dnetlib.dhp.utils.ISLookupClientFactory;
import eu.dnetlib.enabling.is.lookup.rmi.ISLookUpService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.data.mdstore.manager.common.model.MDStoreVersion;
import eu.dnetlib.dhp.model.mdstore.MetadataRecord;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AggregationJobTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static SparkSession spark;

    private static Path workingDir;

    private static Encoder<MetadataRecord> encoder;

    private static final String encoding = "XML";
    private static final String dateOfCollection = System.currentTimeMillis() + "";
    private static final String xpath = "//*[local-name()='header']/*[local-name()='identifier']";
    private static String provenance;

    private static final Logger log = LoggerFactory.getLogger(AggregationJobTest.class);

    @BeforeAll
    public static void beforeAll() throws IOException {
        provenance = IOUtils.toString(AggregationJobTest.class.getResourceAsStream("/eu/dnetlib/dhp/collection/provenance.json"));
        workingDir = Files.createTempDirectory(AggregationJobTest.class.getSimpleName());
        log.info("using work dir {}", workingDir);

        SparkConf conf = new SparkConf();

        conf.setAppName(AggregationJobTest.class.getSimpleName());

        conf.setMaster("local[*]");
        conf.set("spark.driver.host", "localhost");
        conf.set("hive.metastore.local", "true");
        conf.set("spark.ui.enabled", "false");
        conf.set("spark.sql.warehouse.dir", workingDir.toString());
        conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());

        encoder = Encoders.bean(MetadataRecord.class);
        spark = SparkSession
                .builder()
                .appName(AggregationJobTest.class.getSimpleName())
                .config(conf)
                .getOrCreate();
    }

    @AfterAll
    public static void afterAll() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
        spark.stop();
    }

    @Test
    @Order(1)
    public void testGenerateNativeStoreSparkJobRefresh() throws Exception {

        MDStoreVersion mdStoreV1 = prepareVersion("/eu/dnetlib/dhp/collection/mdStoreVersion_1.json");
        FileUtils.forceMkdir(new File(mdStoreV1.getHdfsPath()));

        IOUtils
                .copy(
                        getClass().getResourceAsStream("/eu/dnetlib/dhp/collection/sequence_file"),
                        new FileOutputStream(mdStoreV1.getHdfsPath() + "/sequence_file"));

        GenerateNativeStoreSparkJob
                .main(
                        new String[]{
                                "-isSparkSessionManaged", Boolean.FALSE.toString(),
                                "-encoding", encoding,
                                "-dateOfCollection", dateOfCollection,
                                "-provenance", provenance,
                                "-xpath", xpath,
                                "-mdStoreVersion", OBJECT_MAPPER.writeValueAsString(mdStoreV1),
                                "-readMdStoreVersion", "",
                                "-workflowId", "abc"
                        });

        verify(mdStoreV1);
    }

    @Test
    @Order(2)
    public void testGenerateNativeStoreSparkJobIncremental() throws Exception {

        MDStoreVersion mdStoreV2 = prepareVersion("/eu/dnetlib/dhp/collection/mdStoreVersion_2.json");
        FileUtils.forceMkdir(new File(mdStoreV2.getHdfsPath()));

        IOUtils
                .copy(
                        getClass().getResourceAsStream("/eu/dnetlib/dhp/collection/sequence_file"),
                        new FileOutputStream(mdStoreV2.getHdfsPath() + "/sequence_file"));

        MDStoreVersion mdStoreV1 = prepareVersion("/eu/dnetlib/dhp/collection/mdStoreVersion_1.json");

        GenerateNativeStoreSparkJob
                .main(
                        new String[]{
                                "-isSparkSessionManaged", Boolean.FALSE.toString(),
                                "-encoding", encoding,
                                "-dateOfCollection", dateOfCollection,
                                "-provenance", provenance,
                                "-xpath", xpath,
                                "-mdStoreVersion", OBJECT_MAPPER.writeValueAsString(mdStoreV2),
                                "-readMdStoreVersion", OBJECT_MAPPER.writeValueAsString(mdStoreV1),
                                "-workflowId", "abc"
                        });

        verify(mdStoreV2);
    }


    //@Test
    @Order(3)
    public void testTransformSparkJob() throws Exception {

        MDStoreVersion mdStoreV2 = prepareVersion("/eu/dnetlib/dhp/collection/mdStoreVersion_2.json");
        MDStoreVersion mdStoreCleanedVersion = prepareVersion("/eu/dnetlib/dhp/collection/mdStoreCleanedVersion.json");

        TransformSparkJobNode.main(new String[]{
                "-isSparkSessionManaged", Boolean.FALSE.toString(),
                "-dateOfTransformation", dateOfCollection,
                "-mdstoreInputVersion", OBJECT_MAPPER.writeValueAsString(mdStoreV2),
                "-mdstoreOutputVersion", OBJECT_MAPPER.writeValueAsString(mdStoreCleanedVersion),
                "-transformationPlugin", "XSLT_TRANSFORM",
                "-isLookupUrl", "https://dev-openaire.d4science.org/is/services/isLookUp",
                "-transformationRuleId", "183dde52-a69b-4db9-a07e-1ef2be105294_VHJhbnNmb3JtYXRpb25SdWxlRFNSZXNvdXJjZXMvVHJhbnNmb3JtYXRpb25SdWxlRFNSZXNvdXJjZVR5cGU="});

    }

    protected void verify(MDStoreVersion mdStoreVersion) throws IOException {
        Assertions.assertTrue(new File(mdStoreVersion.getHdfsPath()).exists());

        final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        long seqFileSize = sc
                .sequenceFile(mdStoreVersion.getHdfsPath() + "/sequence_file", IntWritable.class, Text.class)
                .count();

        final Dataset<MetadataRecord> mdstore = spark.read().load(mdStoreVersion.getHdfsPath() + "/store").as(encoder);
        long mdStoreSize = mdstore.count();

        long declaredSize = Long.parseLong(IOUtils.toString(new FileReader(mdStoreVersion.getHdfsPath() + "/size")));

        Assertions.assertEquals(seqFileSize, declaredSize, "the size must be equal");
        Assertions.assertEquals(seqFileSize, mdStoreSize, "the size must be equal");

        long uniqueIds = mdstore
                .map((MapFunction<MetadataRecord, String>) MetadataRecord::getId, Encoders.STRING())
                .distinct()
                .count();

        Assertions.assertEquals(seqFileSize, uniqueIds, "the size must be equal");
    }

    private MDStoreVersion prepareVersion(String filename) throws IOException {
        MDStoreVersion mdstore = OBJECT_MAPPER
                .readValue(IOUtils.toString(getClass().getResource(filename)), MDStoreVersion.class);
        mdstore.setHdfsPath(String.format(mdstore.getHdfsPath(), workingDir.toString()));
        return mdstore;
    }

}
