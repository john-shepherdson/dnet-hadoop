package eu.dnetlib.dhp.collection.plugin.oai;

import eu.dnetlib.dhp.collection.worker.utils.HttpConnector;
import java.util.Iterator;

public class OaiIteratorFactory {

  private HttpConnector httpConnector;

  public Iterator<String> newIterator(
      final String baseUrl,
      final String mdFormat,
      final String set,
      final String fromDate,
      final String untilDate) {
    return new OaiIterator(baseUrl, mdFormat, set, fromDate, untilDate, getHttpConnector());
  }

  private HttpConnector getHttpConnector() {
    if (httpConnector == null) httpConnector = new HttpConnector();
    return httpConnector;
  }
}
