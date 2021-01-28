
package eu.dnetlib.dhp.collection.plugin;

import java.util.stream.Stream;

import eu.dnetlib.collector.worker.model.ApiDescriptor;
import eu.dnetlib.dhp.collection.worker.CollectorException;

public interface CollectorPlugin {

	Stream<String> collect(ApiDescriptor api) throws CollectorException;
}
