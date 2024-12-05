package eu.dnetlib.dhp.actionmanager.raid;

import eu.dnetlib.dhp.actionmanager.opencitations.CreateOpenCitationsASTest;
import eu.dnetlib.dhp.actionmanager.raid.model.RAiDEntity;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OtherResearchProduct;
import eu.dnetlib.dhp.schema.oaf.Relation;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static eu.dnetlib.dhp.actionmanager.Constants.OBJECT_MAPPER;
import static java.nio.file.Files.createTempDirectory;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class GenerateRAiDActionSetJobTest {
    private static String input_path;
    private static String output_path;
    static SparkSession spark;

    @BeforeEach
    void setUp() throws Exception {

        input_path = Paths
                .get(GenerateRAiDActionSetJobTest.class.getResource("/eu/dnetlib/dhp/actionmanager/raid/raid_example.json").toURI())
                .toFile()
                .getAbsolutePath();

        output_path = createTempDirectory(GenerateRAiDActionSetJobTest.class.getSimpleName() + "-")
                .toAbsolutePath()
                .toString();

        SparkConf conf = new SparkConf();
        conf.setAppName(GenerateRAiDActionSetJobTest.class.getSimpleName());

        conf.setMaster("local[*]");
        conf.set("spark.driver.host", "localhost");
        conf.set("hive.metastore.local", "true");
        conf.set("spark.ui.enabled", "false");
        conf.set("spark.sql.warehouse.dir", output_path);
        conf.set("hive.metastore.warehouse.dir", output_path);

        spark = SparkSession
                .builder()
                .appName(GenerateRAiDActionSetJobTest.class.getSimpleName())
                .config(conf)
                .getOrCreate();
    }

    @AfterAll
    static void cleanUp() throws Exception {
        FileUtils.deleteDirectory(new File(output_path));
    }

    @Test
    @Disabled
    void testProcessRAiDEntities() {
        GenerateRAiDActionSetJob.processRAiDEntities(spark, input_path, output_path + "/test_raid_action_set");

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        JavaRDD<? extends Oaf> result = sc
                .sequenceFile(output_path + "/test_raid_action_set", Text.class, Text.class)
                .map(value -> OBJECT_MAPPER.readValue(value._2().toString(), AtomicAction.class))
                .map(AtomicAction::getPayload);

        assertEquals(80, result.count());
    }

    @Test
    void testPrepareRAiD() {

        List<AtomicAction<? extends Oaf>> atomicActions = GenerateRAiDActionSetJob.prepareRAiD(new RAiDEntity(
                "-92190526",
                Arrays.asList("Berli, Justin", "Le Mao, Bérénice", "Guillaume Touya", "Wenclik, Laura", "Courtial, Azelle", "Muehlenhaus, Ian", "Justin Berli", "Touya, Guillaume", "Gruget, Maïeul", "Azelle Courtial", "Ian Muhlenhaus", "Maïeul Gruget", "Marion Dumont", "Maïeul GRUGET", "Cécile Duchêne"),
                "2021-09-10",
                "2024-02-16",
                Arrays.asList("cartography, zoom, pan, desert fog", "Road network", "zooming", "Pan-scalar maps", "pan-scalar map", "Python library", "QGIS", "map design", "landmarks", "Cartes transscalaires", "anchor", "disorientation", "[INFO]Computer Science [cs]", "[SHS.GEO]Humanities and Social Sciences/Geography", "cognitive cartography", "eye-tracking", "Computers in Earth Sciences", "Topographic map", "National Mapping Agency", "General Medicine", "Geography, Planning and Development", "multi-scales", "pan-scalar maps", "Selection", "cartography", "General Earth and Planetary Sciences", "progressiveness", "map generalisation", "Eye-tracker", "zoom", "algorithms", "Map Design", "cartography, map generalisation, zoom, multi-scale map", "Interactive maps", "Map generalisation", "Earth and Planetary Sciences (miscellaneous)", "Cartographic generalization", "rivers", "Benchmark", "General Environmental Science", "open source", "drawing", "Constraint", "Multi-scale maps"),
                Arrays.asList("Where do people look at during multi-scale map tasks?", "FogDetector survey raw data", "Collection of cartographic disorientation stories", "Anchorwhat dataset", "BasqueRoads: A Benchmark for Road Network Selection", "Progressive river network selection for pan-scalar maps", "BasqueRoads, a dataset to benchmark road selection algorithms", "Missing the city for buildings? A critical review of pan-scalar map generalization and design in contemporary zoomable maps", "Empirical approach to advance the generalisation of multi-scale maps", "L'Alpe d'Huez: a dataset to benchmark topographic map generalisation", "eye-tracking data from a survey on zooming in a pan-scalar map", "Material of the experiment 'More is Less' from the MapMuxing project", "Cartagen4py, an open source Python library for map generalisation", "L’Alpe d’Huez: A Benchmark for Topographic Map Generalisation"),
                Arrays.asList("50|doi_dedup___::6915135e0aa39f913394513f809ae58a", "50|doi_dedup___::754e3c283639bc6e104c925ff3e34007", "50|doi_dedup___::13517477f3c1261d57a3364363ce6ce0", "50|doi_dedup___::675b16c73accc4e7242bbb4ed9b3724a", "50|doi_dedup___::94ce09906b2d7d37eb2206cea8a50153", "50|dedup_wf_002::cc575d5ca5651ff8c3029a3a76e7e70a", "50|doi_dedup___::c5e52baddda17c755d1bae012a97dc13", "50|doi_dedup___::4f5f38c9e08fe995f7278963183f8ad4", "50|doi_dedup___::a9bc4453273b2d02648a5cb453195042", "50|doi_dedup___::5e893dc0cb7624a33f41c9b428bd59f7", "50|doi_dedup___::c1ecdef48fd9be811a291deed950e1c5", "50|doi_dedup___::9e93c8f2d97c35de8a6a57a5b53ef283", "50|dedup_wf_002::d08be0ed27b13d8a880e891e08d093ea", "50|doi_dedup___::f8d8b3b9eddeca2fc0e3bc9e63996555"),
                "Exploring Multi-Scale Map Generalization and Design",
                "This project aims to advance the generalization of multi-scale maps by investigating the impact of different design elements on user experience. The research involves collecting and analyzing data from various sources, including surveys, eye-tracking studies, and user experiments. The goal is to identify best practices for map generalization and design, with a focus on reducing disorientation and improving information retrieval during exploration. The project has led to the development of several datasets, including BasqueRoads, AnchorWhat, and L'Alpe d'Huez, which can be used to benchmark road selection algorithms and topographic map generalization techniques. The research has also resulted in the creation of a Python library, Cartagen4py, for map generalization. The findings of this project have the potential to improve the design and usability of multi-scale maps, making them more effective tools for navigation and information retrieval."
        ));

        OtherResearchProduct orp = (OtherResearchProduct) atomicActions.get(0).getPayload();
        Relation rel = (Relation) atomicActions.get(1).getPayload();

        assertEquals("Exploring Multi-Scale Map Generalization and Design", orp.getTitle().get(0).getValue());
        assertEquals("50|raid________::759a564ce5cc7360cab030c517c7366b", rel.getSource());
        assertEquals("50|doi_dedup___::6915135e0aa39f913394513f809ae58a", rel.getTarget());

    }

}
