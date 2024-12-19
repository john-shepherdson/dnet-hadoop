
package eu.dnetlib.dhp.collection.plugin.csv;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;

public class HttpCsvCollectorPlugin implements CollectorPlugin {

	private final HttpClientParams clientParams;

	public HttpCsvCollectorPlugin(final HttpClientParams clientParams) {
		this.clientParams = clientParams;
	}

	@Override
	public Stream<String> collect(final ApiDescriptor api, final AggregatorReport report) throws CollectorException {

		final String baseUrl = api.getBaseUrl();

		final String header = api.getParams().get("header");
		final String separator = api.getParams().get("separator");
		final String identifier = api.getParams().get("identifier");
		final String quote = api.getParams().get("quote");

		final Iterator<String> iterator = new HttpCsvIterator(baseUrl, header, separator, identifier, quote, this.clientParams);

		final Spliterator<String> spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED);

		return StreamSupport.stream(spliterator, false);
	}

}
