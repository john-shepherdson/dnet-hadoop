package eu.dnetlib.dhp.graph;

import eu.dnetlib.dhp.graph.utils.ContextMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class XmlRecordFactoryTest {

    private static final Log log = LogFactory.getLog(XmlRecordFactoryTest.class);

    private Path testDir;

    @Before
    public void setup() throws IOException {
        testDir = Files.createTempDirectory(getClass().getSimpleName());
        log.info("created test directory " + testDir.toString());
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(testDir.toFile());
        log.info("deleted test directory " + testDir.toString());
    }

    @Test
    public void  testXmlSerialization() throws Exception {

        final SparkSession spark = SparkSession
                .builder()
                .appName(SparkXmlRecordBuilderJob.class.getSimpleName())
                .master("local[*]")
                .getOrCreate();

        final String inputDir = testDir.toString() + "/3_joined_entities";
        FileUtils.forceMkdir(new File(inputDir));
        FileUtils.copyFile(new File("/Users/claudio/Downloads/joined_entities-part-00000"), new File(inputDir + "/joined_entities-part-00000"));

        final ContextMapper ctx = ContextMapper.fromIS("https://dev-openaire.d4science.org:443/is/services/isLookUp");

        final GraphJoiner g = new GraphJoiner(spark, ctx, inputDir, testDir.toString());

        g.asXML();
    }

}
