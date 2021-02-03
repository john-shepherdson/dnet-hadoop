
package eu.dnetlib.dhp.collection.plugin.oai;

import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.collection.worker.CollectorException;
import eu.dnetlib.dhp.collection.worker.utils.CollectorPluginErrorLogList;
import eu.dnetlib.dhp.collection.worker.utils.HttpConnector;
import eu.dnetlib.dhp.collection.worker.utils.XmlCleaner;

public class OaiIterator implements Iterator<String> {

	private static final Logger log = LoggerFactory.getLogger(OaiIterator.class);

	private final Queue<String> queue = new PriorityBlockingQueue<>();
	private final SAXReader reader = new SAXReader();

	private final String baseUrl;
	private final String set;
	private final String mdFormat;
	private final String fromDate;
	private final String untilDate;
	private String token;
	private boolean started;
	private final HttpConnector httpConnector;
	private CollectorPluginErrorLogList errorLogList;

	public OaiIterator(
		final String baseUrl,
		final String mdFormat,
		final String set,
		final String fromDate,
		final String untilDate,
		final HttpConnector httpConnector,
		final CollectorPluginErrorLogList errorLogList) {
		this.baseUrl = baseUrl;
		this.mdFormat = mdFormat;
		this.set = set;
		this.fromDate = fromDate;
		this.untilDate = untilDate;
		this.started = false;
		this.httpConnector = httpConnector;
		this.errorLogList = errorLogList;
	}

	private void verifyStarted() {
		if (!this.started) {
			this.started = true;
			try {
				this.token = firstPage();
			} catch (final CollectorException e) {
				throw new RuntimeException(e);
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
					throw new RuntimeException(e);
				}
			}
			return res;
		}
	}

	@Override
	public void remove() {
	}

	private String firstPage() throws CollectorException {
		try {
			String url = baseUrl + "?verb=ListRecords&metadataPrefix=" + URLEncoder.encode(mdFormat, "UTF-8");
			if (set != null && !set.isEmpty()) {
				url += "&set=" + URLEncoder.encode(set, "UTF-8");
			}
			if (fromDate != null && fromDate.matches("\\d{4}-\\d{2}-\\d{2}")) {
				url += "&from=" + URLEncoder.encode(fromDate, "UTF-8");
			}
			if (untilDate != null && untilDate.matches("\\d{4}-\\d{2}-\\d{2}")) {
				url += "&until=" + URLEncoder.encode(untilDate, "UTF-8");
			}
			log.info("Start harvesting using url: " + url);

			return downloadPage(url);
		} catch (final UnsupportedEncodingException e) {
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
					+ URLEncoder.encode(resumptionToken, "UTF-8"));
		} catch (final UnsupportedEncodingException e) {
			throw new CollectorException(e);
		}
	}

	private String downloadPage(final String url) throws CollectorException {

		final String xml = httpConnector.getInputSource(url, errorLogList);
		Document doc;
		try {
			doc = reader.read(new StringReader(xml));
		} catch (final DocumentException e) {
			log.warn("Error parsing xml, I try to clean it: " + xml, e);
			final String cleaned = XmlCleaner.cleanAllEntities(xml);
			try {
				doc = reader.read(new StringReader(cleaned));
			} catch (final DocumentException e1) {
				final String resumptionToken = extractResumptionToken(xml);
				if (resumptionToken == null) {
					throw new CollectorException("Error parsing cleaned document:" + cleaned, e1);
				}
				return resumptionToken;
			}
		}

		final Node errorNode = doc.selectSingleNode("/*[local-name()='OAI-PMH']/*[local-name()='error']");
		if (errorNode != null) {
			final String code = errorNode.valueOf("@code");
			if ("noRecordsMatch".equalsIgnoreCase(code.trim())) {
				log.warn("noRecordsMatch for oai call: " + url);
				return null;
			} else {
				throw new CollectorException(code + " - " + errorNode.getText());
			}
		}

		for (final Object o : doc.selectNodes("//*[local-name()='ListRecords']/*[local-name()='record']")) {
			queue.add(((Node) o).asXML());
		}

		return doc.valueOf("//*[local-name()='resumptionToken']");
	}

	public CollectorPluginErrorLogList getErrorLogList() {
		return errorLogList;
	}
}
