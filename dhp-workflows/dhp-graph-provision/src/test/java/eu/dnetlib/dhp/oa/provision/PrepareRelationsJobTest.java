
package eu.dnetlib.dhp.oa.provision;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.oa.provision.model.ProvisionModelSupport;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class PrepareRelationsJobTest {

	private static final Logger log = LoggerFactory.getLogger(PrepareRelationsJobTest.class);

	public static final String SUBRELTYPE = "subRelType";
	public static final String OUTCOME = "outcome";
	public static final String PARTICIPATION = "participation";
	public static final String AFFILIATION = "affiliation";

	private static SparkSession spark;

	private static Path workingDir;

	@BeforeAll
	public static void setUp() throws IOException {
		workingDir = Files.createTempDirectory(PrepareRelationsJobTest.class.getSimpleName());
		log.info("using work dir {}", workingDir);

		SparkConf conf = new SparkConf();

		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.registerKryoClasses(ProvisionModelSupport.getModelClasses());

		spark = SparkSession
			.builder()
			.appName(PrepareRelationsJobTest.class.getSimpleName())
			.master("local[*]")
			.config(conf)
			.getOrCreate();
	}

	@AfterAll
	public static void afterAll() throws IOException {
		FileUtils.deleteDirectory(workingDir.toFile());
		spark.stop();
	}

	@Test
	void testRunPrepareRelationsJob(@TempDir Path testPath) throws Exception {

		final int maxRelations = 20;
		PrepareRelationsJob
			.main(
				new String[] {
					"-isSparkSessionManaged", Boolean.FALSE.toString(),
					"-inputRelationsPath", getClass().getResource("relations.gz").getPath(),
					"-outputPath", testPath.toString(),
					"-relPartitions", "10",
					"-relationFilter", "asd",
					"-sourceMaxRelations", String.valueOf(maxRelations),
					"-targetMaxRelations", String.valueOf(maxRelations * 100)
				});

		Dataset<Relation> out = spark
			.read()
			.parquet(testPath.toString())
			.as(Encoders.bean(Relation.class))
			.cache();

		assertEquals(maxRelations, out.count());

		Dataset<Row> freq = out
			.toDF()
			.cube(SUBRELTYPE)
			.count()
			.filter((FilterFunction<Row>) value -> !value.isNullAt(0));

		log.info(freq.collectAsList().toString());

		long outcome = getRows(freq, OUTCOME).get(0).getAs("count");
		long participation = getRows(freq, PARTICIPATION).get(0).getAs("count");
		long affiliation = getRows(freq, AFFILIATION).get(0).getAs("count");

		assertEquals(outcome, participation);
		assertTrue(outcome > affiliation);
		assertTrue(participation > affiliation);

		assertEquals(7, outcome);
		assertEquals(7, participation);
		assertEquals(6, affiliation);
	}

	protected List<Row> getRows(Dataset<Row> freq, String col) {
		return freq.filter(freq.col(SUBRELTYPE).equalTo(col)).collectAsList();
	}

}
