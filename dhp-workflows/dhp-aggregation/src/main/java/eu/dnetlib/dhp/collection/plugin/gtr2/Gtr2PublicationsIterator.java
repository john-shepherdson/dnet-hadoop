package eu.dnetlib.dhp.collection.plugin.gtr2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;
import eu.dnetlib.dhp.common.collection.HttpConnector2;

public class Gtr2PublicationsIterator implements Iterator<String> {

	public static final int PAGE_SIZE = 20;

	private static final Logger log = LoggerFactory.getLogger(Gtr2PublicationsIterator.class);

	private final HttpConnector2 connector;
	private static final DateTimeFormatter simpleDateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");

	private static final int MAX_ATTEMPTS = 10;

	private final String baseUrl;
	private int currPage;
	private int endPage;
	private boolean incremental = false;
	private DateTime fromDate;

	private final Map<String, String> cache = new HashMap<>();

	private final Queue<String> queue = new LinkedList<>();

	private String nextElement;

	public Gtr2PublicationsIterator(final String baseUrl, final String fromDate, final String startPage, final String endPage,
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
			log.debug("FETCHING PAGE + " + this.currPage + "/" + this.endPage);
			this.queue.addAll(fetchPage(this.currPage++));
		}
		this.nextElement = this.queue.poll();
	}

	private List<String> fetchPage(final int pageNumber) {

		final List<String> res = new ArrayList<>();
		try {
			final Document doc = loadURL(cleanURL(this.baseUrl + "/outcomes/publications?p=" + pageNumber), 0);

			if (this.endPage == Integer.MAX_VALUE) {
				this.endPage = NumberUtils.toInt(doc.valueOf("/*/@*[local-name() = 'totalPages']"));
			}

			for (final Object po : doc.selectNodes("//*[local-name() = 'publication']")) {
				final Element mainEntity = (Element) ((Element) po).detach();

				if (filterIncremental(mainEntity)) {
					res.add(expandMainEntity(mainEntity));
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

	private void addLinkedEntities(final Element master, final String relType, final Element newRoot, final Function<Document, Element> mapper) {

		for (final Object o : master.selectNodes(".//*[local-name()='link']")) {
			final String rel = ((Element) o).valueOf("@*[local-name()='rel']");
			final String href = ((Element) o).valueOf("@*[local-name()='href']");

			if (relType.equals(rel) && StringUtils.isNotBlank(href)) {
				final String cacheKey = relType + "#" + href;
				if (this.cache.containsKey(cacheKey)) {
					try {
						log.debug(" * from cache (" + relType + "): " + href);
						newRoot.add(DocumentHelper.parseText(this.cache.get(cacheKey)).getRootElement());
					} catch (final DocumentException e) {
						log.error("Error retrieving cache element: " + cacheKey, e);
						throw new RuntimeException("Error retrieving cache element: " + cacheKey, e);
					}
				} else {
					final Document doc = loadURL(cleanURL(href), 0);
					final Element elem = mapper.apply(doc);
					newRoot.add(elem);
					this.cache.put(cacheKey, elem.asXML());
				}

			}
		}
	}

	private boolean filterIncremental(final Element e) {
		if (!this.incremental || isAfter(e.valueOf("@*[local-name() = 'created']"), this.fromDate)
				|| isAfter(e.valueOf("@*[local-name() = 'updated']"), this.fromDate)) {
			return true;
		}
		return false;
	}

	private String expandMainEntity(final Element mainEntity) {
		final Element newRoot = DocumentHelper.createElement("doc");
		newRoot.add(mainEntity);
		addLinkedEntities(mainEntity, "PROJECT", newRoot, this::asProjectElement);
		return DocumentHelper.createDocument(newRoot).asXML();
	}

	private Element asProjectElement(final Document doc) {
		final Element newOrg = DocumentHelper.createElement("project");
		newOrg.addElement("id").setText(doc.valueOf("/*/@*[local-name()='id']"));
		newOrg.addElement("code").setText(doc.valueOf("//*[local-name()='identifier' and @*[local-name()='type'] = 'RCUK']"));
		newOrg.addElement("title").setText(doc.valueOf("//*[local-name()='title']"));
		return newOrg;
	}

	private static String cleanURL(final String url) {
		String cleaned = url;
		if (cleaned.contains("gtr.gtr")) {
			cleaned = cleaned.replace("gtr.gtr", "gtr");
		}
		if (cleaned.startsWith("http://")) {
			cleaned = cleaned.replaceFirst("http://", "https://");
		}
		return cleaned;
	}

	private Document loadURL(final String cleanUrl, final int attempt) {
		try {
			log.debug("  * Downloading Url: " + cleanUrl);
			final byte[] bytes = this.connector.getInputSource(cleanUrl).getBytes("UTF-8");
			return DocumentHelper.parseText(new String(bytes));
		} catch (final Throwable e) {
			log.error("Error dowloading url: " + cleanUrl + ", attempt = " + attempt, e);
			if (attempt >= MAX_ATTEMPTS) { throw new RuntimeException("Error dowloading url: " + cleanUrl, e); }
			try {
				Thread.sleep(60000); // I wait for a minute
			} catch (final InterruptedException e1) {
				throw new RuntimeException("Error dowloading url: " + cleanUrl, e);
			}
			return loadURL(cleanUrl, attempt + 1);
		}
	}

	private DateTime parseDate(final String s) {
		return DateTime.parse(s.contains("T") ? s.substring(0, s.indexOf("T")) : s, simpleDateTimeFormatter);
	}

	private boolean isAfter(final String d, final DateTime fromDate) {
		return StringUtils.isNotBlank(d) && parseDate(d).isAfter(fromDate);
	}
}
