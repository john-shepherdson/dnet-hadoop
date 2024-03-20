
package eu.dnetlib.dhp.collection.plugin.oai;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Node;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.collection.plugin.utils.XmlCleaner;
import eu.dnetlib.dhp.common.aggregation.AggregatorReport;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpConnector2;

public class OaiIterator implements Iterator<String> {

	private static final Logger log = LoggerFactory.getLogger(OaiIterator.class);

	private static final String REPORT_PREFIX = "oai:";
	public static final String UTF_8 = "UTF-8";

	private final Queue<String> queue = new PriorityBlockingQueue<>();

	private final String baseUrl;
	private final String set;
	private final String mdFormat;
	private final String fromDate;
	private final String untilDate;
	private String token;
	private boolean started;
	private final HttpConnector2 httpConnector;
	private final AggregatorReport report;

	public OaiIterator(
		final String baseUrl,
		final String mdFormat,
		final String set,
		final String fromDate,
		final String untilDate,
		final HttpConnector2 httpConnector,
		final AggregatorReport report) {
		this.baseUrl = baseUrl;
		this.mdFormat = mdFormat;
		this.set = set;
		this.fromDate = fromDate;
		this.untilDate = untilDate;
		this.started = false;
		this.httpConnector = httpConnector;
		this.report = report;
	}

	private void verifyStarted() {
		if (!this.started) {
			this.started = true;
			try {
				this.token = firstPage();
			} catch (final CollectorException e) {
				throw new IllegalStateException(e);
			}
		}
	}

	@Override
	public boolean hasNext() {
		synchronized (queue) {
			verifyStarted();
			return !queue.isEmpty();
		}
	}

	@Override
	public String next() {
		synchronized (queue) {
			verifyStarted();
			final String res = queue.poll();
			while (queue.isEmpty() && token != null && !token.isEmpty()) {
				try {
					token = otherPages(token);
				} catch (final CollectorException e) {
					throw new IllegalStateException(e);
				}
			}
			return res;
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	private String firstPage() throws CollectorException {
		try {
			String url = baseUrl + "?verb=ListRecords&metadataPrefix=" + URLEncoder.encode(mdFormat, UTF_8);
			if (set != null && !set.isEmpty()) {
				url += "&set=" + URLEncoder.encode(set, UTF_8);
			}
			if (fromDate != null && (fromDate.matches(OaiCollectorPlugin.DATE_REGEX)
				|| fromDate.matches(OaiCollectorPlugin.UTC_DATETIME_REGEX))) {
				url += "&from=" + URLEncoder.encode(fromDate, UTF_8);
			}
			if (untilDate != null && (untilDate.matches(OaiCollectorPlugin.DATE_REGEX)
				|| untilDate.matches(OaiCollectorPlugin.UTC_DATETIME_REGEX))) {
				url += "&until=" + URLEncoder.encode(untilDate, UTF_8);
			}
			log.info("Start harvesting using url: {}", url);

			return downloadPage(url);
		} catch (final UnsupportedEncodingException e) {
			report.put(e.getClass().getName(), e.getMessage());
			throw new CollectorException(e);
		}
	}

	private String extractResumptionToken(final String xml) {

		final String s = StringUtils.substringAfter(xml, "<resumptionToken");
		if (s == null) {
			return null;
		}

		final String result = StringUtils.substringBetween(s, ">", "</");
		if (result == null) {
			return null;
		}
		return result.trim();
	}

	private String otherPages(final String resumptionToken) throws CollectorException {
		try {
			return downloadPage(
				baseUrl
					+ "?verb=ListRecords&resumptionToken="
					+ URLEncoder.encode(resumptionToken, UTF_8));
		} catch (final UnsupportedEncodingException e) {
			report.put(e.getClass().getName(), e.getMessage());
			throw new CollectorException(e);
		}
	}

	private String downloadPage(final String url) throws CollectorException {

		final String xml = httpConnector.getInputSource(url, report);
		Document doc;
		try {
			doc = DocumentHelper.parseText(xml);
		} catch (final DocumentException e) {
			log.warn("Error parsing xml, I try to clean it. {}", e.getMessage());
			report.put(e.getClass().getName(), e.getMessage());
			final String cleaned = XmlCleaner.cleanAllEntities(xml);
			try {
				doc = DocumentHelper.parseText(cleaned);
			} catch (final DocumentException e1) {
				final String resumptionToken = extractResumptionToken(xml);
				if (resumptionToken == null) {
					report.put(e1.getClass().getName(), e1.getMessage());
					throw new CollectorException("Error parsing cleaned document:\n" + cleaned, e1);
				}
				return resumptionToken;
			}
		}

		final Node errorNode = doc.selectSingleNode("/*[local-name()='OAI-PMH']/*[local-name()='error']");
		if (errorNode != null) {
			final String code = errorNode.valueOf("@code").trim();
			if ("noRecordsMatch".equalsIgnoreCase(code)) {
				final String msg = "noRecordsMatch for oai call : " + url;
				log.warn(msg);
				report.put(REPORT_PREFIX + code, msg);
				return null;
			} else {
				final String msg = code + " - " + errorNode.getText();
				report.put(REPORT_PREFIX + "error", msg);
				throw new CollectorException(msg);
			}
		}

		for (final Object o : doc.selectNodes("//*[local-name()='ListRecords']/*[local-name()='record']")) {
			final StringWriter sw = new StringWriter();
			final XMLWriter writer = new XMLWriter(sw, OutputFormat.createPrettyPrint());
			try {
				writer.write((Node) o);
				queue.add(sw.toString());
			} catch (IOException e) {
				report.put(e.getClass().getName(), e.getMessage());
				throw new CollectorException("Error parsing XML record:\n" + ((Node) o).asXML(), e);
			}
		}

		return doc.valueOf("//*[local-name()='resumptionToken']");
	}

	public AggregatorReport getReport() {
		return report;
	}
}
