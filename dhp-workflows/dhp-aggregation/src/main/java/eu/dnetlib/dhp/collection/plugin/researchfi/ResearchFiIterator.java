package eu.dnetlib.dhp.collection.plugin.researchfi;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.math.NumberUtils;
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
	private int currPage;
	private int nPages;

	private final Queue<String> queue = new PriorityBlockingQueue<>();

	public ResearchFiIterator(final String baseUrl, final String authToken) {
		this.baseUrl = baseUrl;
		this.authToken = authToken;
		this.currPage = 0;
		this.nPages = 0;
	}

	private void verifyStarted() {
		if (this.currPage == 0) {
			try {
				nextCall();
			} catch (final CollectorException e) {
				throw new IllegalStateException(e);
			}
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
			while (this.queue.isEmpty() && (this.currPage < this.nPages)) {
				try {
					nextCall();
				} catch (final CollectorException e) {
					throw new IllegalStateException(e);
				}
			}
			return res;
		}
	}

	private void nextCall() throws CollectorException {

		this.currPage += 1;

		try (final CloseableHttpClient client = HttpClients.createDefault()) {
			final String url;
			if (!this.baseUrl.contains("?")) {
				url = String.format("%s?PageNumber=%d&PageSize=%d", this.baseUrl, this.currPage, PAGE_SIZE);
			} else if (!this.baseUrl.contains("PageSize=")) {
				url = String.format("%s&PageNumber=%d&PageSize=%d", this.baseUrl, this.currPage, PAGE_SIZE);
			} else {
				url = String.format("%s&PageNumber=%d", this.baseUrl, this.currPage);
			}
			log.info("Calling url: " + url);

			final HttpGet req = new HttpGet(url);
			req.addHeader("Authorization", "Bearer " + this.authToken);
			try (final CloseableHttpResponse response = client.execute(req)) {
				for (final Header header : response.getAllHeaders()) {
					log.debug("HEADER: " + header.getName() + " = " + header.getValue());
					if ("x-page-count".equals(header.getName())) {
						final int totalPages = NumberUtils.toInt(header.getValue());
						if (this.nPages != totalPages) {
							this.nPages = NumberUtils.toInt(header.getValue());
							log.info("Total pages: " + totalPages);
						}
					}
				}

				final String content = IOUtils.toString(response.getEntity().getContent());
				final JSONArray jsonArray = new JSONArray(content);

				jsonArray.forEach(obj -> this.queue.add(JsonUtils.convertToXML(obj.toString())));
			}
		} catch (final Throwable e) {
			log.warn("Error obtaining access token", e);
			throw new CollectorException("Error obtaining access token", e);
		}
	}

}
