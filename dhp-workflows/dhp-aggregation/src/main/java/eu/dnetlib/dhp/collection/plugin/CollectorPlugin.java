
package eu.dnetlib.dhp.collection.plugin;

import java.util.stream.Stream;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.CollectorException;
import eu.dnetlib.dhp.collection.CollectorPluginReport;

public interface CollectorPlugin {

	Stream<String> collect(ApiDescriptor api, CollectorPluginReport report) throws CollectorException;

}
