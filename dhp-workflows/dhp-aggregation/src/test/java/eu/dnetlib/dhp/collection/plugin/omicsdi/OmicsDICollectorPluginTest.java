
package eu.dnetlib.dhp.collection.plugin.omicsdi;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;

@Disabled
class OmicsDICollectorPluginTest {

	private OmicsDICollectorPlugin plugin;
	private ApiDescriptor api;

	@BeforeEach
	void setUp() throws Exception {
		this.api = new ApiDescriptor();
		this.api.setBaseUrl("https://www.omicsdi.org/ws");
		this.api.setProtocol(CollectorPlugin.NAME.omicsdi.name());
		this.api.getParams().put("pageSize", "1000");

		this.plugin = new OmicsDICollectorPlugin(new HttpClientParams());
	}

	@Test
	void testComplete() throws CollectorException {
		this.plugin.collect(this.api, null).forEach(s -> System.out.println(s));
	}

}
