
package eu.dnetlib.dhp.collector.worker;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.collection.ApiDescriptor;

@Disabled
public class CollectorWorkerApplicationTests {

	@Test
	public void testCollectionOAI() throws Exception {
		final ApiDescriptor api = new ApiDescriptor();
		api.setId("oai");
		api.setProtocol("oai");
		api.setBaseUrl("http://www.revista.vocesdelaeducacion.com.mx/index.php/index/oai");
		api.getParams().put("format", "oai_dc");
		ObjectMapper mapper = new ObjectMapper();
		assertNotNull(mapper.writeValueAsString(api));
	}

	private ApiDescriptor getApi() {
		final ApiDescriptor api = new ApiDescriptor();
		api.setId("oai");
		api.setProtocol("oai");
		api.setBaseUrl("http://www.revista.vocesdelaeducacion.com.mx/index.php/index/oai");
		api.getParams().put("format", "oai_dc");
		return api;
	}
}
