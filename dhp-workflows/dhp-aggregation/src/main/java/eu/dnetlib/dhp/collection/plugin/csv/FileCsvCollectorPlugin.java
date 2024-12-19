
package eu.dnetlib.dhp.collection.plugin.csv;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.hadoop.fs.FileSystem;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;

public class FileCsvCollectorPlugin implements CollectorPlugin {

	private final FileSystem fileSystem;

	public FileCsvCollectorPlugin(final FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}

	@Override
	public Stream<String> collect(final ApiDescriptor api, final AggregatorReport report) throws CollectorException {

		final String baseUrl = api.getBaseUrl();

		final String header = api.getParams().get("header");
		final String separator = api.getParams().get("separator");
		final String identifier = api.getParams().get("identifier");
		final String quote = api.getParams().get("quote");

		final Iterator<String> iterator = new FileCsvIterator(baseUrl, header, separator, identifier, quote, this.fileSystem);

		final Spliterator<String> spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED);

		return StreamSupport.stream(spliterator, false);
	}

}
