
package eu.dnetlib.dhp.actionmanager.project.httpconnector;

import eu.dnetlib.dhp.collection.worker.DnetCollectorException;
import eu.dnetlib.dhp.collection.worker.utils.HttpConnector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.ssl.SSLContextBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class HttpConnectorTest {

	private static final Log log = LogFactory.getLog(HttpConnectorTest.class);
	private static HttpConnector connector;

	private static final String URL = "http://cordis.europa.eu/data/reference/cordisref-H2020topics.xlsx";
	private static final String URL_MISCONFIGURED_SERVER = "https://www.alexandria.unisg.ch/cgi/oai2?verb=Identify";
	private static final String URL_GOODSNI_SERVER = "https://air.unimi.it/oai/openaire?verb=Identify";

	private static final SSLContextBuilder sslContextBuilder = new SSLContextBuilder();
	private static SSLConnectionSocketFactory sslSocketFactory;

	@BeforeAll
	public static void setUp() {
		connector = new HttpConnector();
	}

	@Test

	public void testGetInputSource() throws DnetCollectorException {
		System.out.println(connector.getInputSource(URL));
	}

	@Test
	public void testGoodServers() throws DnetCollectorException {
		System.out.println(connector.getInputSource(URL_GOODSNI_SERVER));
	}

}
