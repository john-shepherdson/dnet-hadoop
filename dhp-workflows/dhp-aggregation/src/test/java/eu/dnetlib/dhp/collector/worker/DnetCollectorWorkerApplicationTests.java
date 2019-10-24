package eu.dnetlib.dhp.collector.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.collector.worker.model.ApiDescriptor;
import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.collection.worker.DnetCollectorWorker;
import eu.dnetlib.dhp.collection.worker.utils.CollectorPluginFactory;
import eu.dnetlib.message.Message;
import eu.dnetlib.message.MessageManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;


public class DnetCollectorWorkerApplicationTests {


    private ArgumentApplicationParser argumentParser = mock(ArgumentApplicationParser.class);
    private MessageManager messageManager = mock(MessageManager.class);

    private DnetCollectorWorker worker;
    @Before
    public void setup() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        final String apiJson = mapper.writeValueAsString(getApi());
        when(argumentParser.get("apidescriptor")).thenReturn(apiJson);
        when(argumentParser.get("namenode")).thenReturn("file://tmp/test.seq");
        when(argumentParser.get("hdfsPath")).thenReturn("/tmp/file.seq");
        when(argumentParser.get("userHDFS")).thenReturn("sandro");
        when(argumentParser.get("workflowId")).thenReturn("sandro");
        when(argumentParser.get("rabbitOngoingQueue")).thenReturn("sandro");

        when(messageManager.sendMessage(any(Message.class), anyString(), anyBoolean(),anyBoolean())).thenAnswer(a -> {
            System.out.println("sent message: "+a.getArguments()[0]);
            return true;
        });
        when(messageManager.sendMessage(any(Message.class), anyString())).thenAnswer(a -> {
            System.out.println("Called");
            return true;
        });
        worker = new DnetCollectorWorker(new CollectorPluginFactory(), argumentParser, messageManager);
    }


    @After
    public void dropDown(){
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
