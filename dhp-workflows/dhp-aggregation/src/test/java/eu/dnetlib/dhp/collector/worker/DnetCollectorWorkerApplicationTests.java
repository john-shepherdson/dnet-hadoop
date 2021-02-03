
package eu.dnetlib.dhp.collector.worker;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Path;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.collection.worker.CollectorWorker;
import eu.dnetlib.dhp.collection.worker.utils.CollectorPluginFactory;
import eu.dnetlib.dhp.collector.worker.model.ApiDescriptor;

@Disabled
public class DnetCollectorWorkerApplicationTests {

	@Test
	public void testFindPlugin() throws Exception {
		final CollectorPluginFactory collectorPluginEnumerator = new CollectorPluginFactory();
		assertNotNull(collectorPluginEnumerator.getPluginByProtocol("oai"));
		assertNotNull(collectorPluginEnumerator.getPluginByProtocol("OAI"));
	}

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

	@Test
	public void testFeeding(@TempDir Path testDir) throws Exception {

		System.out.println(testDir.toString());
		CollectorWorker worker = new CollectorWorker(getApi(),
			"file://" + testDir.toString() + "/file.seq", testDir.toString() + "/file.seq");
		worker.collect();

		// TODO create ASSERT HERE
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
