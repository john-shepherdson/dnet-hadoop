
package eu.dnetlib.dhp.common.api;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
class ZenodoAPIClientTest {

	private final String URL_STRING = "https://sandbox.zenodo.org/api/deposit/depositions";
	private final String ACCESS_TOKEN = "OzzOsyucEIHxCEfhlpsMo3myEiwpCza3trCRL7ddfGTAK9xXkIP2MbXd6Vg4";

	private final String CONCEPT_REC_ID = "657113";

	private final String depositionId = "674915";

	@Test
	void testUploadOldDeposition() throws IOException, MissingConceptDoiException {
		ZenodoAPIClient client = new ZenodoAPIClient(URL_STRING,
			ACCESS_TOKEN);
		Assertions.assertEquals(200, client.uploadOpenDeposition(depositionId));

		File file = new File(getClass()
			.getResource("/eu/dnetlib/dhp/common/api/COVID-19.json.gz")
			.getPath());

		InputStream is = new FileInputStream(file);

		Assertions.assertEquals(200, client.uploadIS(is, "COVID-19.json.gz"));

		String metadata = IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/common/api/metadata.json"));

		Assertions.assertEquals(200, client.sendMretadata(metadata));

		Assertions.assertEquals(202, client.publish());

	}

	@Test
	void testNewDeposition() throws IOException {

		ZenodoAPIClient client = new ZenodoAPIClient(URL_STRING,
			ACCESS_TOKEN);
		Assertions.assertEquals(201, client.newDeposition());

		File file = new File(getClass()
			.getResource("/eu/dnetlib/dhp/common/api/newVersion")
			.getPath());

		InputStream is = new FileInputStream(file);

//		Assertions.assertEquals(200, client.uploadIS(is, "COVID-19.json.gz"));

		String metadata = IOUtils.toString(getClass().getResourceAsStream("/eu/dnetlib/dhp/common/api/metadata.json"));

		Assertions.assertEquals(200, client.sendMretadata(metadata));

		// Assertions.assertEquals(202, client.publish());

	}

	@Test
	void testNewVersionNewName() throws IOException, MissingConceptDoiException {

		ZenodoAPIClient client = new ZenodoAPIClient(URL_STRING,
			ACCESS_TOKEN);

		Assertions.assertEquals(201, client.newVersion(CONCEPT_REC_ID));

		File file = new File(getClass()
			.getResource("/eu/dnetlib/dhp/common/api/newVersion")
			.getPath());

		InputStream is = new FileInputStream(file);

		Assertions.assertEquals(200, client.uploadIS(is, "newVersion_deposition"));

		Assertions.assertEquals(202, client.publish());

	}

	@Test
	void testNewVersionOldName() throws IOException, MissingConceptDoiException {

		ZenodoAPIClient client = new ZenodoAPIClient(URL_STRING,
			ACCESS_TOKEN);

		Assertions.assertEquals(201, client.newVersion(CONCEPT_REC_ID));

		File file = new File(getClass()
			.getResource("/eu/dnetlib/dhp/common/api/newVersion2")
			.getPath());

		InputStream is = new FileInputStream(file);

		Assertions.assertEquals(200, client.uploadIS(is, "newVersion_deposition"));

		Assertions.assertEquals(202, client.publish());

	}

	@Test
	void depositBigFile() throws MissingConceptDoiException, IOException {
		ZenodoAPIClient client = new ZenodoAPIClient(URL_STRING,
			ACCESS_TOKEN);

		Assertions.assertEquals(201, client.newDeposition());

		File file = new File("/Users/miriam.baglioni/Desktop/EOSC_DUMP/publication.tar");
//		File file = new File(getClass()
//				.getResource("/eu/dnetlib/dhp/common/api/newVersion2")
//				.getPath());

		InputStream is = new FileInputStream(file);

		Assertions.assertEquals(200, client.uploadIS(is, "newVersion_deposition"));

		// Assertions.assertEquals(202, client.publish());
	}

}
