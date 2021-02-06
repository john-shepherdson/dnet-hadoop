
package eu.dnetlib.dhp.collection.worker;

import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.collection.plugin.oai.OaiCollectorPlugin;

public class CollectorPluginFactory {

	public static CollectorPlugin getPluginByProtocol(final HttpClientParams clientParams, final String protocol)
		throws UnknownCollectorPluginException {
		if (protocol == null)
			throw new UnknownCollectorPluginException("protocol cannot be null");
		switch (protocol.toLowerCase().trim()) {
			case "oai":
				return new OaiCollectorPlugin(clientParams);
			default:
				throw new UnknownCollectorPluginException("Unknown protocol");
		}
	}
}
