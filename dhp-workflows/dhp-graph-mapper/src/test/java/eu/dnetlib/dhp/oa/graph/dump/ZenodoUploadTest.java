
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ZenodoUploadTest {

	@Test
	public void testConnection() throws IOException {
		APIClient s = new APIClient(
		//	"https://sandbox.zenodo.org/api/deposit/depositions?access_token=5ImUj0VC1ICg4ifK5dc3AGzJhcfAB4osxrFlsr8WxHXxjaYgCE0hY8HZcDoe");
				"https://sandbox.zenodo.org/api/deposit/depositions");

		Assertions.assertEquals(201, s.connect());

		final String sourcePath = getClass()
				.getResource("/eu/dnetlib/dhp/oa/graph/dump/zenodo/ni")
				.getPath();

		s.upload(sourcePath, "Neuroinformatics");

	}
//
//	@Test
//	public void testGet() throws IOException {
//		APIClient s = new APIClient("https://sandbox.zenodo.org/api/deposit/depositions");
//
//		s.get();
//	}

	@Test
	public void testUpload() throws IOException {

		APIClient s = new APIClient("https://sandbox.zenodo.org/api/deposit/depositions?access_token=5ImUj0VC1ICg4ifK5dc3AGzJhcfAB4osxrFlsr8WxHXxjaYgCE0hY8HZcDoe");
		final String sourcePath = getClass()
				.getResource("/eu/dnetlib/dhp/oa/graph/dump/zenodo/ni")
				.getPath();

		s.upload(sourcePath, "Neuroinformatics");


	}
}
