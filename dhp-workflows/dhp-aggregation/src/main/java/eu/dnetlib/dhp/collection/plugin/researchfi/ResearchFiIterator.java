
package eu.dnetlib.dhp.collection.plugin.researchfi;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;

import eu.dnetlib.dhp.collection.plugin.utils.JsonUtils;
import eu.dnetlib.dhp.common.collection.CollectorException;

public class ResearchFiIterator implements Iterator<String> {

	private static final Log log = LogFactory.getLog(ResearchFiIterator.class);

	private static final int PAGE_SIZE = 100;

	private final String baseUrl;
	private final String authToken;
	private String nextUrl;
	private int nCalls = 0;

	private final Queue<String> queue = new PriorityBlockingQueue<>();

	public ResearchFiIterator(final String baseUrl, final String authToken) {
		this.baseUrl = baseUrl;
		this.authToken = authToken;
		this.nextUrl = null;
	}

	private void verifyStarted() {

		try {
			if (this.nCalls == 0) {
				this.nextUrl = invokeUrl(this.baseUrl);
			}
		} catch (final CollectorException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public boolean hasNext() {
		synchronized (this.queue) {
			verifyStarted();
			return !this.queue.isEmpty();
		}
	}

	@Override
	public String next() {
		synchronized (this.queue) {
			verifyStarted();
			final String res = this.queue.poll();
			while (this.queue.isEmpty() && StringUtils.isNotBlank(this.nextUrl)) {
				try {
					this.nextUrl = invokeUrl(this.nextUrl);
				} catch (final CollectorException e) {
					throw new IllegalStateException(e);
				}
			}
			return res;
		}
	}

	private String invokeUrl(final String url) throws CollectorException {

		this.nCalls += 1;
		String next = null;

		log.info("Calling url: " + url);

		try (final CloseableHttpClient client = HttpClients.createDefault()) {

			final HttpGet req = new HttpGet(url);
			req.addHeader("Authorization", "Bearer " + this.authToken);
			try (final CloseableHttpResponse response = client.execute(req)) {
				for (final Header header : response.getAllHeaders()) {
					log.debug("HEADER: " + header.getName() + " = " + header.getValue());
					if ("link".equals(header.getName())) {
						final String s = StringUtils.substringBetween(header.getValue(), "<", ">");
						final String token = StringUtils
							.substringBefore(StringUtils.substringAfter(s, "NextPageToken="), "&");

						if (this.baseUrl.contains("?")) {
							next = this.baseUrl + "&NextPageToken=" + token;
						} else {
							next = this.baseUrl + "?NextPageToken=" + token;
						}
					}
				}

				final String content = IOUtils.toString(response.getEntity().getContent());
				final JSONArray jsonArray = new JSONArray(content);

				jsonArray.forEach(obj -> this.queue.add(JsonUtils.convertToXML(obj.toString())));
			}

			return next;

		} catch (final Throwable e) {
			log.warn("Error calling url: " + url, e);
			throw new CollectorException("Error calling url: " + url, e);
		}
	}

}
