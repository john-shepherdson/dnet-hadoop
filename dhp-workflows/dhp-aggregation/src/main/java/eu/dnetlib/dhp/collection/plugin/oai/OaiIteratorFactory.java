
package eu.dnetlib.dhp.collection.plugin.oai;

import java.util.Iterator;

import eu.dnetlib.dhp.collection.worker.utils.CollectorPluginErrorLogList;
import eu.dnetlib.dhp.collection.worker.utils.HttpConnector;

public class OaiIteratorFactory {

	private HttpConnector httpConnector;

	public Iterator<String> newIterator(
		final String baseUrl,
		final String mdFormat,
		final String set,
		final String fromDate,
		final String untilDate,
		final CollectorPluginErrorLogList errorLogList) {
		return new OaiIterator(baseUrl, mdFormat, set, fromDate, untilDate, getHttpConnector(), errorLogList);
	}

	private HttpConnector getHttpConnector() {
		if (httpConnector == null)
			httpConnector = new HttpConnector();
		return httpConnector;
	}
}
