
package eu.dnetlib.dhp.collector.worker;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

import java.io.File;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.collector.worker.model.ApiDescriptor;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.collection.worker.CollectorWorker;
import eu.dnetlib.dhp.collection.worker.utils.CollectorPluginFactory;
import eu.dnetlib.message.Message;
import eu.dnetlib.message.MessageManager;

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
		CollectorWorker worker = new CollectorWorker(new CollectorPluginFactory(), getApi(),
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
