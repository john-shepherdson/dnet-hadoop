
package eu.dnetlib.dhp.collection.plugin.gtr2;

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

public class Gtr2PublicationsCollectorPlugin implements CollectorPlugin {

	private final HttpClientParams clientParams;

	public Gtr2PublicationsCollectorPlugin(final HttpClientParams clientParams) {
		this.clientParams = clientParams;
	}

	@Override
	public Stream<String> collect(final ApiDescriptor api, final AggregatorReport report) throws CollectorException {

		final String baseUrl = api.getBaseUrl();
		final String startPage = api.getParams().get("startPage");
		final String endPage = api.getParams().get("endPage");
		final String fromDate = api.getParams().get("fromDate");

		if ((fromDate != null) && !fromDate.matches("\\d{4}-\\d{2}-\\d{2}")) { throw new CollectorException("Invalid date (YYYY-MM-DD): " + fromDate); }

		final Iterator<String> iterator = new Gtr2PublicationsIterator(baseUrl, fromDate, startPage, endPage, this.clientParams);
		final Spliterator<String> spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED);

		return StreamSupport.stream(spliterator, false);
	}

}
