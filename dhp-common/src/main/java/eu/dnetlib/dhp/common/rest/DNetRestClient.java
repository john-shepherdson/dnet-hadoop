
package eu.dnetlib.dhp.common.rest;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DNetRestClient {

	private static final Logger log = LoggerFactory.getLogger(DNetRestClient.class);

	private static ObjectMapper mapper = new ObjectMapper();

	public static <T> T doGET(final String url, Class<T> clazz) throws Exception {
		final HttpGet httpGet = new HttpGet(url);
		return doHTTPRequest(httpGet, clazz);
	}

	public static String doGET(final String url) throws Exception {
		final HttpGet httpGet = new HttpGet(url);
		return doHTTPRequest(httpGet);
	}

	public static <V> String doPOST(final String url, V objParam) throws Exception {
		final HttpPost httpPost = new HttpPost(url);

		if (objParam != null) {
			final StringEntity entity = new StringEntity(mapper.writeValueAsString(objParam));
			httpPost.setEntity(entity);
			httpPost.setHeader("Accept", "application/json");
			httpPost.setHeader("Content-type", "application/json");
		}
		return doHTTPRequest(httpPost);
	}

	public static <T, V> T doPOST(final String url, V objParam, Class<T> clazz) throws Exception {
		return mapper.readValue(doPOST(url, objParam), clazz);
	}

	private static String doHTTPRequest(final HttpUriRequest r) throws Exception {
		CloseableHttpClient client = HttpClients.createDefault();

		log.info("performing HTTP request, method {} on URI {}", r.getMethod(), r.getURI().toString());
		log
			.info(
				"request headers: {}",
				Arrays
					.asList(r.getAllHeaders())
					.stream()
					.map(h -> h.getName() + ":" + h.getValue())
					.collect(Collectors.joining(",")));

		CloseableHttpResponse response = client.execute(r);
		return IOUtils.toString(response.getEntity().getContent());
	}

	private static <T> T doHTTPRequest(final HttpUriRequest r, Class<T> clazz) throws Exception {
		return mapper.readValue(doHTTPRequest(r), clazz);
	}
}
