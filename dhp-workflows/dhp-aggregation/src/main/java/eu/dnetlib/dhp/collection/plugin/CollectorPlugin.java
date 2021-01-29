
package eu.dnetlib.dhp.collection.plugin;

import java.util.stream.Stream;

import eu.dnetlib.dhp.collection.worker.CollectorException;
import eu.dnetlib.dhp.collector.worker.model.ApiDescriptor;

public interface CollectorPlugin {

	Stream<String> collect(ApiDescriptor api) throws CollectorException;
}
