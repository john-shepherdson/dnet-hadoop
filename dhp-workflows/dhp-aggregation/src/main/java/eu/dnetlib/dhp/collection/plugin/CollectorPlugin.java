
package eu.dnetlib.dhp.collection.plugin;

import java.util.stream.Stream;

import eu.dnetlib.collector.worker.model.ApiDescriptor;
import eu.dnetlib.dhp.collection.worker.DnetCollectorException;

public interface CollectorPlugin {

	Stream<String> collect(ApiDescriptor api) throws DnetCollectorException;
}
