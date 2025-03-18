
package eu.dnetlib.dhp.actionmanager.raid;

import static java.nio.file.Files.createTempDirectory;

import static eu.dnetlib.dhp.actionmanager.Constants.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

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

import eu.dnetlib.dhp.actionmanager.opencitations.CreateOpenCitationsASTest;
import eu.dnetlib.dhp.actionmanager.raid.model.RAiDEntity;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OtherResearchProduct;
import eu.dnetlib.dhp.schema.oaf.Relation;
import scala.Tuple2;

public class GenerateRAiDActionSetJobTest {
	private static String input_path;
	private static String output_path;
	private static String baseUrl;
	private static String graphBasePath;
	static SparkSession spark;

	@BeforeEach
	void setUp() throws Exception {

		input_path = Paths
			.get(
				GenerateRAiDActionSetJobTest.class
					.getResource("/eu/dnetlib/dhp/actionmanager/raid/raid_example.json")
					.toURI())
			.toFile()
			.getAbsolutePath();

		output_path = createTempDirectory(GenerateRAiDActionSetJobTest.class.getSimpleName() + "-")
			.toAbsolutePath()
			.toString();

		baseUrl = "https://baseurl/";
		graphBasePath = Paths
			.get(
				GenerateRAiDActionSetJobTest.class
					.getResource("/eu/dnetlib/dhp/actionmanager/raid/")
					.toURI())
			.toFile()
			.getAbsolutePath();

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
	void rawRAiDToGraphEntitiesTest() {

		List<? extends Oaf> graphEntities = GenerateRAiDActionSetJob
			.rawRAiDtoGraphEntities(
				new RAiDEntity(
					"-92190526",
					"Exploring Multi-Scale Map Generalization and Design",
					"This project aims to advance the generalization of multi-scale maps by investigating the impact of different design elements on user experience. The research involves collecting and analyzing data from various sources, including surveys, eye-tracking studies, and user experiments. The goal is to identify best practices for map generalization and design, with a focus on reducing disorientation and improving information retrieval during exploration. The project has led to the development of several datasets, including BasqueRoads, AnchorWhat, and L'Alpe d'Huez, which can be used to benchmark road selection algorithms and topographic map generalization techniques. The research has also resulted in the creation of a Python library, Cartagen4py, for map generalization. The findings of this project have the potential to improve the design and usability of multi-scale maps, making them more effective tools for navigation and information retrieval.",
					Arrays
						.asList(
							"50|doi_dedup___::6915135e0aa39f913394513f809ae58a",
							"50|doi_dedup___::754e3c283639bc6e104c925ff3e34007",
							"50|doi_dedup___::13517477f3c1261d57a3364363ce6ce0",
							"50|doi_dedup___::675b16c73accc4e7242bbb4ed9b3724a",
							"50|doi_dedup___::94ce09906b2d7d37eb2206cea8a50153",
							"50|dedup_wf_002::cc575d5ca5651ff8c3029a3a76e7e70a",
							"50|doi_dedup___::c5e52baddda17c755d1bae012a97dc13",
							"50|doi_dedup___::4f5f38c9e08fe995f7278963183f8ad4",
							"50|doi_dedup___::a9bc4453273b2d02648a5cb453195042",
							"50|doi_dedup___::5e893dc0cb7624a33f41c9b428bd59f7",
							"50|doi_dedup___::c1ecdef48fd9be811a291deed950e1c5",
							"50|doi_dedup___::9e93c8f2d97c35de8a6a57a5b53ef283",
							"50|dedup_wf_002::d08be0ed27b13d8a880e891e08d093ea",
							"50|doi_dedup___::f8d8b3b9eddeca2fc0e3bc9e63996555"),
					"2021-09-10",
					"2024-02-16"),
				"https://baseurl/");

		OtherResearchProduct orp = (OtherResearchProduct) graphEntities.get(0);
		Relation rel = (Relation) graphEntities.get(1);

		assertEquals("Exploring Multi-Scale Map Generalization and Design", orp.getTitle().get(0).getValue());
		assertEquals(
			"https://baseurl/raid________::759a564ce5cc7360cab030c517c7366b", orp.getInstance().get(0).getUrl().get(0));
		assertEquals("50|raid________::759a564ce5cc7360cab030c517c7366b", rel.getSource());
		assertEquals("50|doi_dedup___::6915135e0aa39f913394513f809ae58a", rel.getTarget());

	}

	@Test
	void raidEntitiesToAtomicActionsTest() {

		JavaRDD<AtomicAction<? extends Oaf>> atomicActions = GenerateRAiDActionSetJob
			.raidEntitiesToAtomicActions(spark, input_path, baseUrl, graphBasePath);

		JavaRDD<Relation> relations = atomicActions
			.filter(aa -> aa.getClazz().equals(Relation.class))
			.map(AtomicAction::getPayload)
			.map(p -> (Relation) p);
		JavaRDD<OtherResearchProduct> raids = atomicActions
			.filter(aa -> aa.getClazz().equals(OtherResearchProduct.class))
			.map(AtomicAction::getPayload)
			.map(p -> (OtherResearchProduct) p);

		assertEquals(6, raids.count());
		assertEquals(80, relations.count()); // all relations
		assertEquals(4, relations.filter(r -> r.getRelType().equals(ModelConstants.RESULT_ORGANIZATION)).count());
		assertEquals(2, relations.filter(r -> r.getRelType().equals(ModelConstants.RESULT_PROJECT)).count());
		assertEquals(74, relations.filter(r -> r.getRelType().equals(ModelConstants.RESULT_RESULT)).count());

	}

}
