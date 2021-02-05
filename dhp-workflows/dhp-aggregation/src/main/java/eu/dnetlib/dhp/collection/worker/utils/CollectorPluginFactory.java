
package eu.dnetlib.dhp.collection.worker.utils;

import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.collection.plugin.oai.OaiCollectorPlugin;
import eu.dnetlib.dhp.collection.worker.CollectorException;

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
