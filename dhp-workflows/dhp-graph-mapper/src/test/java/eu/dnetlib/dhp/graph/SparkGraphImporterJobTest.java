package eu.dnetlib.dhp.graph;

import org.apache.commons.io.FileUtils;
import org.junit.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class SparkGraphImporterJobTest {

    private static final long MAX = 1000L;
    private Path testDir;

    @Before
    public void setup() throws IOException {
        testDir = Files.createTempDirectory(getClass().getSimpleName());
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(testDir.toFile());
    }

    @Test
    @Ignore
    public void  testImport() throws Exception {
        SparkGraphImporterJob.main(new String[] {
                "-mt", "local[*]",
                "-i", getClass().getResource("/eu/dnetlib/dhp/dhp-sample/part-m-00010").getPath(),
                "-o", testDir.toString()});

        SparkGraphImportCounterTest.countEntities(testDir.toString()).forEach(t -> {
            System.out.println(t);
            //Assert.assertEquals(String.format("mapped %s must be %s", t._1(), MAX), MAX, t._2().longValue());
        });
    }
}
