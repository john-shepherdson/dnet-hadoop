
package eu.dnetlib.dhp.collection.plugin.oai;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import eu.dnetlib.dhp.aggregation.common.AggregatorReport;
import eu.dnetlib.dhp.collection.ApiDescriptor;
import eu.dnetlib.dhp.collection.CollectorException;
import eu.dnetlib.dhp.collection.HttpClientParams;
import eu.dnetlib.dhp.collection.plugin.CollectorPlugin;

public class OaiCollectorPlugin implements CollectorPlugin {

	private static final String FORMAT_PARAM = "format";
	private static final String OAI_SET_PARAM = "set";
	private static final Object OAI_FROM_DATE_PARAM = "fromDate";
	private static final Object OAI_UNTIL_DATE_PARAM = "untilDate";

	private OaiIteratorFactory oaiIteratorFactory;

	private HttpClientParams clientParams;

	public OaiCollectorPlugin(HttpClientParams clientParams) {
		this.clientParams = clientParams;
	}

	@Override
	public Stream<String> collect(final ApiDescriptor api, final AggregatorReport report)
		throws CollectorException {
		final String baseUrl = api.getBaseUrl();
		final String mdFormat = api.getParams().get(FORMAT_PARAM);
		final String setParam = api.getParams().get(OAI_SET_PARAM);
		final String fromDate = api.getParams().get(OAI_FROM_DATE_PARAM);
		final String untilDate = api.getParams().get(OAI_UNTIL_DATE_PARAM);

		final List<String> sets = new ArrayList<>();
		if (setParam != null) {
			sets
				.addAll(
					Lists.newArrayList(Splitter.on(",").omitEmptyStrings().trimResults().split(setParam)));
		}
		if (sets.isEmpty()) {
			// If no set is defined, ALL the sets must be harvested
			sets.add("");
		}

		if (baseUrl == null || baseUrl.isEmpty()) {
			throw new CollectorException("Param 'baseurl' is null or empty");
		}

		if (mdFormat == null || mdFormat.isEmpty()) {
			throw new CollectorException("Param 'mdFormat' is null or empty");
		}

		if (fromDate != null && !fromDate.matches("\\d{4}-\\d{2}-\\d{2}")
			&& !fromDate.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z")) {
			throw new CollectorException("Invalid date (YYYY-MM-DD or YYYY-MM-DDT00:00:00Z): " + fromDate);
		}

		if (untilDate != null && !untilDate.matches("\\d{4}-\\d{2}-\\d{2}")
			&& !untilDate.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z")) {
			throw new CollectorException("Invalid date (YYYY-MM-DD or YYYY-MM-DDT00:00:00Z): " + untilDate);
		}

		final Iterator<Iterator<String>> iters = sets
			.stream()
			.map(
				set -> getOaiIteratorFactory()
					.newIterator(baseUrl, mdFormat, set, fromDate, untilDate, getClientParams(), report))
			.iterator();

		return StreamSupport
			.stream(
				Spliterators.spliteratorUnknownSize(Iterators.concat(iters), Spliterator.ORDERED), false);
	}

	public OaiIteratorFactory getOaiIteratorFactory() {
		if (oaiIteratorFactory == null) {
			oaiIteratorFactory = new OaiIteratorFactory();
		}
		return oaiIteratorFactory;
	}

	public HttpClientParams getClientParams() {
		return clientParams;
	}

	public void setClientParams(HttpClientParams clientParams) {
		this.clientParams = clientParams;
	}
}
