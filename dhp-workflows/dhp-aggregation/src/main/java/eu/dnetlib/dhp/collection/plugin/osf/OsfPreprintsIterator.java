
package eu.dnetlib.dhp.collection.plugin.osf;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.collection.plugin.utils.JsonUtils;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;
import eu.dnetlib.dhp.common.collection.HttpConnector2;

public class OsfPreprintsIterator implements Iterator<String> {

	private static final Logger log = LoggerFactory.getLogger(OsfPreprintsIterator.class);

	private static final int MAX_ATTEMPTS = 5;

	private final HttpClientParams clientParams;

	private final String baseUrl;
	private final int pageSize;

	private String currentUrl;

	private final Queue<String> recordQueue = new PriorityBlockingQueue<>();

	public OsfPreprintsIterator(
			final String baseUrl,
			final int pageSize,
			final HttpClientParams clientParams) {

		this.clientParams = clientParams;
		this.baseUrl = baseUrl;
		this.pageSize = pageSize;

		initQueue();
	}

	private void initQueue() {
		this.currentUrl = this.baseUrl + "?filter:is_published:d=true&format=json&page[size]=" + this.pageSize;
		log.info("REST calls starting with {}", this.currentUrl);
	}

	@Override
	public boolean hasNext() {
		synchronized (this.recordQueue) {
			while (this.recordQueue.isEmpty() && !this.currentUrl.isEmpty()) {
				try {
					this.currentUrl = downloadPage(this.currentUrl, 0);
				} catch (final CollectorException e) {
					log.debug("CollectorPlugin.next()-Exception: {}", e);
					throw new RuntimeException(e);
				}
			}

			if (!this.recordQueue.isEmpty()) { return true; }

			return false;
		}
	}

	@Override
	public String next() {
		synchronized (this.recordQueue) {
			return this.recordQueue.poll();
		}
	}

	private String downloadPage(final String url, final int attempt) throws CollectorException {

		if (attempt > MAX_ATTEMPTS) { throw new CollectorException("Max Number of attempts reached, url:" + url); }

		if (attempt > 0) {
			final int delay = (attempt * 5000);
			log.debug("Attempt {} with delay {}", attempt, delay);
			try {
				Thread.sleep(delay);
			} catch (final InterruptedException e) {
				new CollectorException(e);
			}
		}

		try {
			log.info("requesting URL [{}]", url);

			final HttpConnector2 connector = new HttpConnector2(this.clientParams);

			final String json = connector.getInputSource(url);
			final String xml = JsonUtils.convertToXML(json);

			final Document doc = DocumentHelper.parseText(xml);

			for (final Object o : doc.selectNodes("/*/*[local-name()='data']")) {
				final Element n = (Element) ((Element) o).detach();

				for (final Object o1 : n.selectNodes(".//contributors//href")) {
					// TODO ADD creators
				}
				for (final Object o1 : n.selectNodes(".//primary_file//href")) {
					// TODO ADD fulltexts
				}

				this.recordQueue.add(DocumentHelper.createDocument(n).asXML());
			}

			return doc.valueOf("/*/*[local-name()='links']/*[local-name()='next']");

		} catch (final Throwable e) {
			log.warn(e.getMessage(), e);
			return downloadPage(url, attempt + 1);
		}

	}

}
