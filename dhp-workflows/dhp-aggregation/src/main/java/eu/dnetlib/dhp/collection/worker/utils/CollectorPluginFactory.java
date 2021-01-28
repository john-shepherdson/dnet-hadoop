
package eu.dnetlib.dhp.collection.worker.utils;

import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.collection.plugin.oai.OaiCollectorPlugin;
import eu.dnetlib.dhp.collection.worker.CollectorException;

public class CollectorPluginFactory {

	public CollectorPlugin getPluginByProtocol(final String protocol) throws CollectorException {
		if (protocol == null)
			throw new CollectorException("protocol cannot be null");
		switch (protocol.toLowerCase().trim()) {
			case "oai":
				return new OaiCollectorPlugin();
			default:
				throw new CollectorException("UNknown protocol");
		}
	}
}
