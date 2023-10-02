
package eu.dnetlib.dhp.swh.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpHeaders;

import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;
import eu.dnetlib.dhp.common.collection.HttpConnector2;

public class SWHConnection {

	HttpConnector2 conn;

	public SWHConnection(HttpClientParams clientParams) {

		// set custom headers
		Map<String, String> headers = new HashMap<String, String>() {
			{
				put(HttpHeaders.ACCEPT, "application/json");
				put(HttpHeaders.AUTHORIZATION, String.format("Bearer %s", SWHConstants.ACCESS_TOKEN));
			}
		};

		clientParams.setHeaders(headers);

		// create http connector
		conn = new HttpConnector2(clientParams);

	}

	public String call(String url) throws CollectorException {
		return conn.getInputSource(url);
	}

}
