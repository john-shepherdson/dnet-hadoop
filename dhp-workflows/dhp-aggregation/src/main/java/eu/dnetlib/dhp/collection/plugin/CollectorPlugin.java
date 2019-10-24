package eu.dnetlib.dhp.collection.plugin;

import eu.dnetlib.collector.worker.model.ApiDescriptor;
import eu.dnetlib.dhp.collection.worker.DnetCollectorException;

import java.util.stream.Stream;

public interface CollectorPlugin {

	Stream<String> collect(ApiDescriptor api) throws DnetCollectorException;
}
