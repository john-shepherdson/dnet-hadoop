package eu.dnetlib.dhp.collection.worker.utils;

import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.collection.plugin.oai.OaiCollectorPlugin;
import eu.dnetlib.dhp.collection.worker.DnetCollectorException;

;


public class CollectorPluginFactory {

    public CollectorPlugin getPluginByProtocol(final String protocol) throws DnetCollectorException {
        if (protocol==null) throw  new DnetCollectorException("protocol cannot be null");
        switch (protocol.toLowerCase().trim()){
            case "oai":
                return new OaiCollectorPlugin();
            default:
                throw  new DnetCollectorException("UNknown protocol");
        }

    }
}
