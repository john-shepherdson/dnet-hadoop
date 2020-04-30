
package eu.dnetlib.dhp.collector.worker;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

import java.io.File;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.collector.worker.model.ApiDescriptor;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.collection.worker.DnetCollectorWorker;
import eu.dnetlib.dhp.collection.worker.utils.CollectorPluginFactory;
import eu.dnetlib.message.Message;
import eu.dnetlib.message.MessageManager;

public class DnetCollectorWorkerApplicationTests {

	private final ArgumentApplicationParser argumentParser = mock(ArgumentApplicationParser.class);
	private final MessageManager messageManager = mock(MessageManager.class);

	private DnetCollectorWorker worker;

	@BeforeEach
	public void setup() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		final String apiJson = mapper.writeValueAsString(getApi());
		when(argumentParser.get("apidescriptor")).thenReturn(apiJson);
		when(argumentParser.get("namenode")).thenReturn("file://tmp/test.seq");
		when(argumentParser.get("hdfsPath")).thenReturn("/tmp/file.seq");
		when(argumentParser.get("userHDFS")).thenReturn("sandro");
		when(argumentParser.get("workflowId")).thenReturn("sandro");
		when(argumentParser.get("rabbitOngoingQueue")).thenReturn("sandro");

		when(messageManager.sendMessage(any(Message.class), anyString(), anyBoolean(), anyBoolean()))
			.thenAnswer(
				a -> {
					System.out.println("sent message: " + a.getArguments()[0]);
					return true;
				});
		when(messageManager.sendMessage(any(Message.class), anyString()))
			.thenAnswer(
				a -> {
					System.out.println("Called");
					return true;
				});
		worker = new DnetCollectorWorker(new CollectorPluginFactory(), argumentParser, messageManager);
	}

	@AfterEach
	public void dropDown() {
		File f = new File("/tmp/file.seq");
		f.delete();
	}

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
	public void testFeeding() throws Exception {
		worker.collect();
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
