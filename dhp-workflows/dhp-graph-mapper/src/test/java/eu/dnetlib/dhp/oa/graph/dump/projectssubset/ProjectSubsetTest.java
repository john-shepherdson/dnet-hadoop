package eu.dnetlib.dhp.oa.graph.dump.projectssubset;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.oa.graph.dump.funderresults.SparkResultLinkedToProject;
import eu.dnetlib.dhp.schema.dump.oaf.community.CommunityResult;
import eu.dnetlib.dhp.schema.dump.oaf.graph.Project;
public class ProjectSubsetTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static SparkSession spark;
    private static Path workingDir;
    private static final Logger log = LoggerFactory
            .getLogger(eu.dnetlib.dhp.oa.graph.dump.projectssubset.ProjectSubsetTest.class);
    private static final HashMap<String, String> map = new HashMap<>();
    @BeforeAll
    public static void beforeAll() throws IOException {
        workingDir = Files
                .createTempDirectory(
                        eu.dnetlib.dhp.oa.graph.dump.projectssubset.ProjectSubsetTest.class.getSimpleName());
        log.info("using work dir {}", workingDir);
        SparkConf conf = new SparkConf();
        conf.setAppName(eu.dnetlib.dhp.oa.graph.dump.projectssubset.ProjectSubsetTest.class.getSimpleName());
        conf.setMaster("local[*]");
        conf.set("spark.driver.host", "localhost");
        conf.set("hive.metastore.local", "true");
        conf.set("spark.ui.enabled", "false");
        conf.set("spark.sql.warehouse.dir", workingDir.toString());
        conf.set("hive.metastore.warehouse.dir", workingDir.resolve("warehouse").toString());
        spark = SparkSession
                .builder()
                .appName(eu.dnetlib.dhp.oa.graph.dump.projectssubset.ProjectSubsetTest.class.getSimpleName())
                .config(conf)
                .getOrCreate();
    }
    @AfterAll
    public static void afterAll() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
        spark.stop();
    }
    @Test
    void testAllNew() throws Exception {
        final String projectListPath = getClass()
                .getResource("/eu/dnetlib/dhp/oa/graph/dump/projectsubset/projectId")
                .getPath();
        final String sourcePath = getClass()
                .getResource("/eu/dnetlib/dhp/oa/graph/dump/projectsubset/allnew/projects")
                .getPath();
        spark
                .read()
                .textFile(projectListPath)
                .write()
                .mode(SaveMode.Overwrite)
                .text(workingDir.toString() + "/projectIds");
        ProjectsSubsetSparkJob.main(new String[] {
                "-isSparkSessionManaged", Boolean.FALSE.toString(),
                "-outputPath", workingDir.toString() + "/projects",
                "-sourcePath", sourcePath,
                "-projectListPath", workingDir.toString() + "/projectIds"
        });
        final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaRDD<Project> tmp = sc
                .textFile(workingDir.toString() + "/projects")
                .map(item -> OBJECT_MAPPER.readValue(item, Project.class));
        Assertions.assertEquals(12, tmp.count());
        Assertions.assertEquals(2, tmp.filter(p -> p.getId().substring(3, 15).equals("aka_________")).count());
        Assertions.assertEquals(2, tmp.filter(p -> p.getId().substring(3, 15).equals("anr_________")).count());
        Assertions.assertEquals(4, tmp.filter(p -> p.getId().substring(3, 15).equals("arc_________")).count());
        Assertions.assertEquals(3, tmp.filter(p -> p.getId().substring(3, 15).equals("conicytf____")).count());
        Assertions.assertEquals(1, tmp.filter(p -> p.getId().substring(3, 15).equals("corda_______")).count());
        Assertions.assertEquals(40, sc.textFile(workingDir.toString() + "/projectIds").count());
    }
    @Test
    void testMatchOne() throws Exception {
        final String projectListPath = getClass()
                .getResource("/eu/dnetlib/dhp/oa/graph/dump/projectsubset/projectId")
                .getPath();
        final String sourcePath = getClass()
                .getResource("/eu/dnetlib/dhp/oa/graph/dump/projectsubset/matchOne/projects")
                .getPath();
        spark
                .read()
                .textFile(projectListPath)
                .write()
                .mode(SaveMode.Overwrite)
                .text(workingDir.toString() + "/projectIds");
        ProjectsSubsetSparkJob.main(new String[] {
                "-isSparkSessionManaged", Boolean.FALSE.toString(),
                "-outputPath", workingDir.toString() + "/projects",
                "-sourcePath", sourcePath,
                "-projectListPath", workingDir.toString() + "/projectIds"
        });
        final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaRDD<Project> tmp = sc
                .textFile(workingDir.toString() + "/projects")
                .map(item -> OBJECT_MAPPER.readValue(item, Project.class));
        Assertions.assertEquals(11, tmp.count());
        Assertions.assertEquals(2, tmp.filter(p -> p.getId().substring(3, 15).equals("aka_________")).count());
        Assertions.assertEquals(2, tmp.filter(p -> p.getId().substring(3, 15).equals("anr_________")).count());
        Assertions.assertEquals(4, tmp.filter(p -> p.getId().substring(3, 15).equals("arc_________")).count());
        Assertions.assertEquals(3, tmp.filter(p -> p.getId().substring(3, 15).equals("conicytf____")).count());
        Assertions.assertEquals(0, tmp.filter(p -> p.getId().substring(3, 15).equals("corda__h2020")).count());
        Assertions.assertEquals(39, sc.textFile(workingDir.toString() + "/projectIds").count());
    }
}