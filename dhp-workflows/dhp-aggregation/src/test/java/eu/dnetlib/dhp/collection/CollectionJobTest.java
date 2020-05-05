
package eu.dnetlib.dhp.collection;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.model.mdstore.MetadataRecord;
import eu.dnetlib.dhp.model.mdstore.Provenance;
import eu.dnetlib.dhp.schema.common.ModelSupport;

public class CollectionJobTest {

	private static SparkSession spark;

	@BeforeAll
	public static void beforeAll() {
		SparkConf conf = new SparkConf();
		conf.setAppName(CollectionJobTest.class.getSimpleName());
		conf.setMaster("local");
		spark = SparkSession.builder().config(conf).getOrCreate();
	}

	@AfterAll
	public static void afterAll() {
		spark.stop();
	}

	@Test
	public void tesCollection(@TempDir Path testDir) throws Exception {
		final Provenance provenance = new Provenance("pippo", "puppa", "ns_prefix");
		Assertions.assertNotNull(new ObjectMapper().writeValueAsString(provenance));

		GenerateNativeStoreSparkJob
			.main(
				new String[] {
					"issm", "true",
					"-w", "wid",
					"-e", "XML",
					"-d", "" + System.currentTimeMillis(),
					"-p", new ObjectMapper().writeValueAsString(provenance),
					"-x", "./*[local-name()='record']/*[local-name()='header']/*[local-name()='identifier']",
					"-i", this.getClass().getResource("/eu/dnetlib/dhp/collection/native.seq").toString(),
					"-o", testDir.toString() + "/store",
					"-t", "true",
					"-ru", "",
					"-rp", "",
					"-rh", "",
					"-ro", "",
					"-rr", ""
				});

		// TODO introduce useful assertions

	}

	@Test
	public void testGenerationMetadataRecord() throws Exception {

		final String xml = IOUtils.toString(this.getClass().getResourceAsStream("./record.xml"));

		final MetadataRecord record = GenerateNativeStoreSparkJob
			.parseRecord(
				xml,
				"./*[local-name()='record']/*[local-name()='header']/*[local-name()='identifier']",
				"XML",
				new Provenance("foo", "bar", "ns_prefix"),
				System.currentTimeMillis(),
				null,
				null);

		assertNotNull(record.getId());
		assertNotNull(record.getOriginalId());
	}

	@Test
	public void TestEquals() throws IOException {

		final String xml = IOUtils.toString(this.getClass().getResourceAsStream("./record.xml"));
		final MetadataRecord record = GenerateNativeStoreSparkJob
			.parseRecord(
				xml,
				"./*[local-name()='record']/*[local-name()='header']/*[local-name()='identifier']",
				"XML",
				new Provenance("foo", "bar", "ns_prefix"),
				System.currentTimeMillis(),
				null,
				null);
		final MetadataRecord record1 = GenerateNativeStoreSparkJob
			.parseRecord(
				xml,
				"./*[local-name()='record']/*[local-name()='header']/*[local-name()='identifier']",
				"XML",
				new Provenance("foo", "bar", "ns_prefix"),
				System.currentTimeMillis(),
				null,
				null);

		record.setBody("ciao");
		record1.setBody("mondo");

		assertNotNull(record);
		assertNotNull(record1);
		assertEquals(record, record1);
	}
}
