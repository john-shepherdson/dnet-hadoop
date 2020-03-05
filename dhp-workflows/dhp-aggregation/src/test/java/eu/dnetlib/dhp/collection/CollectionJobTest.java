package eu.dnetlib.dhp.collection;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.model.mdstore.MetadataRecord;
import eu.dnetlib.dhp.model.mdstore.Provenance;

public class CollectionJobTest {

	private Path testDir;

	@Before
	public void setup() throws IOException {
		testDir = Files.createTempDirectory("dhp-collection");
	}

	@After
	public void teadDown() throws IOException {
		FileUtils.deleteDirectory(testDir.toFile());
	}

	@Test
	public void tesCollection() throws Exception {
		final Provenance provenance = new Provenance("pippo", "puppa", "ns_prefix");
		GenerateNativeStoreSparkJob.main(new String[] {
				"-mt", "local",
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
				"-rr", "" });
		System.out.println(new ObjectMapper().writeValueAsString(provenance));
	}

	@Test
	public void testGenerationMetadataRecord() throws Exception {

		final String xml = IOUtils.toString(this.getClass().getResourceAsStream("./record.xml"));

		final MetadataRecord record = GenerateNativeStoreSparkJob
				.parseRecord(xml, "./*[local-name()='record']/*[local-name()='header']/*[local-name()='identifier']", "XML", new Provenance("foo", "bar",
						"ns_prefix"), System.currentTimeMillis(), null, null);

		assert record != null;
		System.out.println(record.getId());
		System.out.println(record.getOriginalId());

	}

	@Test
	public void TestEquals() throws IOException {

		final String xml = IOUtils.toString(this.getClass().getResourceAsStream("./record.xml"));
		final MetadataRecord record = GenerateNativeStoreSparkJob
				.parseRecord(xml, "./*[local-name()='record']/*[local-name()='header']/*[local-name()='identifier']", "XML", new Provenance("foo", "bar",
						"ns_prefix"), System.currentTimeMillis(), null, null);
		final MetadataRecord record1 = GenerateNativeStoreSparkJob
				.parseRecord(xml, "./*[local-name()='record']/*[local-name()='header']/*[local-name()='identifier']", "XML", new Provenance("foo", "bar",
						"ns_prefix"), System.currentTimeMillis(), null, null);
		assert record != null;
		record.setBody("ciao");
		assert record1 != null;
		record1.setBody("mondo");
		Assert.assertEquals(record, record1);

	}

}
