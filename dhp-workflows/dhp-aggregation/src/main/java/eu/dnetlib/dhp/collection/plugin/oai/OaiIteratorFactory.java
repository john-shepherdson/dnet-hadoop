
package eu.dnetlib.dhp.collection.plugin.oai;

import java.util.Iterator;

import eu.dnetlib.dhp.collection.CollectorPluginReport;
import eu.dnetlib.dhp.collection.HttpClientParams;
import eu.dnetlib.dhp.collection.HttpConnector2;

public class OaiIteratorFactory {

	private HttpConnector2 httpConnector;

	public Iterator<String> newIterator(
		final String baseUrl,
		final String mdFormat,
		final String set,
		final String fromDate,
		final String untilDate,
		final HttpClientParams clientParams,
		final CollectorPluginReport report) {
		return new OaiIterator(baseUrl, mdFormat, set, fromDate, untilDate, getHttpConnector(clientParams), report);
	}

	private HttpConnector2 getHttpConnector(HttpClientParams clientParams) {
		if (httpConnector == null)
			httpConnector = new HttpConnector2(clientParams);
		return httpConnector;
	}
}
