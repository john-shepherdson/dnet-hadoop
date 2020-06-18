
package eu.dnetlib.dhp.oa.graph.dump;

import java.io.IOException;
import java.util.Arrays;

import com.google.gson.Gson;
import eu.dnetlib.dhp.oa.graph.dump.zenodo.Creator;
import eu.dnetlib.dhp.oa.graph.dump.zenodo.Metadata;
import eu.dnetlib.dhp.oa.graph.dump.zenodo.ZenodoModel;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ZenodoUploadTest {

	@Test
	public void testConnection() throws IOException {
		APIClient s = new APIClient(
			// "https://sandbox.zenodo.org/api/deposit/depositions?access_token=5ImUj0VC1ICg4ifK5dc3AGzJhcfAB4osxrFlsr8WxHXxjaYgCE0hY8HZcDoe");
			"https://sandbox.zenodo.org/api/deposit/depositions");

		Assertions.assertEquals(201, s.connect());

		s.upload(getClass()
				.getResource("/eu/dnetlib/dhp/oa/graph/dump/zenodo/ni")
				.getPath(), "Neuroinformatics");

		s.upload(getClass()
				.getResource("/eu/dnetlib/dhp/oa/graph/dump/zenodo/dh-ch")
				.getPath(), "DigitalHumanitiesandCulturalHeritage");

		s.upload(getClass()
				.getResource("/eu/dnetlib/dhp/oa/graph/dump/zenodo/egi")
				.getPath(), "EGI");

		s.upload(getClass()
				.getResource("/eu/dnetlib/dhp/oa/graph/dump/zenodo/science-innovation-policy")
				.getPath(), "ScienceandInnovationPolicyStudies");

//
//
//		String data = "{\"metadata\": {\"title\": \"My first upload\", " +
//				"\"upload_type\": \"poster\", " +
//				"\"description\": \"This is my first upload\", " +
//				"\"creators\": [{\"name\": \"Doe, John\",  " +
//				"\"affiliation': 'Zenodo'}]
//...     }
//... }
//
//
		ZenodoModel zenodo = new ZenodoModel();
		Metadata data = new Metadata();

		data.setTitle("Dump of OpenAIRE Communities related graph");
		data.setUpload_type("dataset");
		data.setDescription("this is a fake uploade done for testing purposes");
		Creator c = new Creator();
		c.setName("Miriam Baglioni");
		c.setAffiliation("CNR _ISTI");
		data.setCreators(Arrays.asList(c));
		zenodo.setMetadata(data);

		s.sendMretadata(new Gson().toJson(zenodo));

		s.publish();

	}


	@Test
	public void testPublish() throws IOException {
		APIClient s = new APIClient("https://sandbox.zenodo.org/api/deposit/depositions");
		s.publish();
	}
	@Test
	public void testUpload() throws IOException {

		APIClient s = new APIClient(
			"https://sandbox.zenodo.org/api/deposit/depositions?access_token=5ImUj0VC1ICg4ifK5dc3AGzJhcfAB4osxrFlsr8WxHXxjaYgCE0hY8HZcDoe");
		final String sourcePath = getClass()
			.getResource("/eu/dnetlib/dhp/oa/graph/dump/zenodo/ni")
			.getPath();

		s.upload(sourcePath, "Neuroinformatics");



	}
}
