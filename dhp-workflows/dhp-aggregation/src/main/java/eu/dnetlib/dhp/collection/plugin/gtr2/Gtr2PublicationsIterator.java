
package eu.dnetlib.dhp.collection.plugin.gtr2;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;
import eu.dnetlib.dhp.common.collection.HttpConnector2;

public class Gtr2PublicationsIterator implements Iterator<String> {

	private static final Logger log = LoggerFactory.getLogger(Gtr2PublicationsIterator.class);

	private final HttpConnector2 connector;
	private static final DateTimeFormatter simpleDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

	private static final int MAX_ATTEMPTS = 10;

	private final String baseUrl;
	private int currPage;
	private int endPage;
	private boolean incremental = false;
	private LocalDate fromDate;
	private final Map<String, String> cache = new HashMap<>();

	private final Queue<String> queue = new LinkedList<>();

	private String nextElement;

	public Gtr2PublicationsIterator(final String baseUrl, final String fromDate, final String startPage,
		final String endPage,
		final HttpClientParams clientParams)
		throws CollectorException {

		this.baseUrl = baseUrl;
		this.currPage = NumberUtils.toInt(startPage, 1);
		this.endPage = NumberUtils.toInt(endPage, Integer.MAX_VALUE);
		this.incremental = StringUtils.isNotBlank(fromDate);
		this.connector = new HttpConnector2(clientParams);

		if (this.incremental) {
			this.fromDate = parseDate(fromDate);
		}

		prepareNextElement();
	}

	@Override
	public boolean hasNext() {
		return this.nextElement != null;
	}

	@Override
	public String next() {
		try {
			return this.nextElement;
		} finally {
			prepareNextElement();
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	private void prepareNextElement() {
		while ((this.currPage <= this.endPage) && this.queue.isEmpty()) {
			log.info("FETCHING PAGE + " + this.currPage + "/" + this.endPage);
			this.queue.addAll(fetchPage(this.currPage++));
		}
		this.nextElement = this.queue.poll();
	}


	private List<String> fetchPage(final int pageNumber) {

		final List<String> res = new ArrayList<>();

		try {
			final Document doc = loadURL(this.baseUrl + "/publication?page=" + pageNumber, 0);

			for (final Object po : doc.selectNodes("//*[local-name() = 'publication']")) {

				final Element mainEntity = (Element) ((Element) po).detach();

				if (filterIncremental(mainEntity)) {
					final String publicationOverview =mainEntity.attributeValue("url");
					res.add(loadURL(publicationOverview, 0).asXML());
				} else {
					log.debug("Skipped entity");
				}

			}
		} catch (final Throwable e) {
			log.error("Exception fetching page " + pageNumber, e);
			throw new RuntimeException("Exception fetching page " + pageNumber, e);
		}

		return res;
	}

	private boolean filterIncremental(final Element e) {
		if (!this.incremental || isAfter(e.valueOf("@*[local-name() = 'created']"), this.fromDate)
			|| isAfter(e.valueOf("@*[local-name() = 'updated']"), this.fromDate)) {
			return true;
		}
		return false;
	}

	private Document loadURL(final String cleanUrl, final int attempt) {
		try (final CloseableHttpClient client = HttpClients.createDefault()) {

			final HttpGet req = new HttpGet(cleanUrl);
			req.setHeader(HttpHeaders.ACCEPT, "application/xml");
			try (final CloseableHttpResponse response = client.execute(req)) {
				if(endPage == Integer.MAX_VALUE)
					for (final Header header : response.getAllHeaders()) {
						log.debug("HEADER: " + header.getName() + " = " + header.getValue());
						if ("Link-Pages".equals(header.getName())) {
							if (Integer.parseInt(header.getValue()) < endPage)
								endPage = Integer.parseInt(header.getValue());
						}
					}

				final String content = IOUtils.toString(response.getEntity().getContent());
				return DocumentHelper.parseText(content);

			}

		} catch (final Throwable e) {
			log.error("Error dowloading url: {}, attempt = {}", cleanUrl, attempt, e);
			if (attempt >= MAX_ATTEMPTS) {
				throw new RuntimeException("Error downloading url: " + cleanUrl, e);
			}
			try {
				Thread.sleep(60000); // I wait for a minute
			} catch (final InterruptedException e1) {
				throw new RuntimeException("Error downloading url: " + cleanUrl, e);
			}
			return loadURL(cleanUrl, attempt + 1);
		}
	}

	private LocalDate parseDate(final String s) {
		return LocalDate.parse(s.contains("T") ? s.substring(0, s.indexOf("T")) : s, simpleDateTimeFormatter);
	}

	private boolean isAfter(final String d, final LocalDate fromDate) {
		return StringUtils.isNotBlank(d) && parseDate(d).isAfter(fromDate);
	}
}
