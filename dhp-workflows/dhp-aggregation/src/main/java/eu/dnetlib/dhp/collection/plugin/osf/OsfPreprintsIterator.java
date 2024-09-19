
package eu.dnetlib.dhp.collection.plugin.osf;

import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import eu.dnetlib.dhp.collection.plugin.utils.JsonUtils;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;
import eu.dnetlib.dhp.common.collection.HttpConnector2;

public class OsfPreprintsIterator implements Iterator<String> {

	private static final Logger log = LoggerFactory.getLogger(OsfPreprintsIterator.class);

	private static final int MAX_ATTEMPTS = 5;

	private final HttpClientParams clientParams;

	private static final String XML_HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
	private static final String EMPTY_XML = XML_HEADER + "<" + JsonUtils.XML_WRAP_TAG + "></" + JsonUtils.XML_WRAP_TAG + ">";

	private final String baseUrl;
	private final int pageSize;

	private int resumptionInt = 0; // integer resumption token (first record to harvest)
	private int resultTotal = -1;
	private String resumptionStr = Integer.toString(this.resumptionInt); // string resumption token (first record to
																		 // harvest
	// or token scanned from results)
	private InputStream resultStream;
	private Transformer transformer;
	private XPath xpath;
	private String query;
	private XPathExpression xprResultTotalPath;
	private XPathExpression xprResumptionPath;
	private XPathExpression xprEntity;
	private final Queue<String> recordQueue = new PriorityBlockingQueue<>();

	public OsfPreprintsIterator(
			final String baseUrl,
			final int pageSize,
			final HttpClientParams clientParams) {

		this.clientParams = clientParams;
		this.baseUrl = baseUrl;
		this.pageSize = pageSize;

		try {
			final TransformerFactory factory = TransformerFactory.newInstance();
			this.transformer = factory.newTransformer();
			this.transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			this.transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "3");
			this.xpath = XPathFactory.newInstance().newXPath();
			this.xprResultTotalPath = this.xpath.compile("/*/*[local-name()='links']/*[local-name()='meta']/*[local-name()='total']");
			this.xprResumptionPath = this.xpath.compile("substring-before(substring-after(/*/*[local-name()='links']/*[local-name()='next'], 'page='), '&')");
			this.xprEntity = this.xpath.compile("/*/*[local-name()='data']");
		} catch (final Exception e) {
			throw new IllegalStateException("xml transformation init failed: " + e.getMessage());
		}

		initQueue();
	}

	private void initQueue() {
		this.query = this.baseUrl + "?filter:is_published:d=true&format=json&page[size]=" + this.pageSize;
		log.info("REST calls starting with {}", this.query);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		synchronized (this.recordQueue) {
			while (this.recordQueue.isEmpty() && !this.query.isEmpty()) {
				try {
					this.query = downloadPage(this.query, 0);
				} catch (final CollectorException e) {
					log.debug("CollectorPlugin.next()-Exception: {}", e);
					throw new RuntimeException(e);
				}
			}

			if (!this.recordQueue.isEmpty()) { return true; }

			return false;
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.util.Iterator#next()
	 */
	@Override
	public String next() {
		synchronized (this.recordQueue) {
			return this.recordQueue.poll();
		}
	}

	/*
	 * download page and return nextQuery (with number of attempt)
	 */
	private String downloadPage(final String query, final int attempt) throws CollectorException {

		if (attempt > MAX_ATTEMPTS) { throw new CollectorException("Max Number of attempts reached, query:" + query); }

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
			String resultJson;
			String resultXml = XML_HEADER;
			String nextQuery = "";
			Node resultNode = null;
			NodeList nodeList = null;

			try {
				log.info("requesting URL [{}]", query);

				final HttpConnector2 connector = new HttpConnector2(this.clientParams);

				resultJson = connector.getInputSource(query);
				resultXml = JsonUtils.convertToXML(resultJson);

				this.resultStream = IOUtils.toInputStream(resultXml, StandardCharsets.UTF_8);

				if (!isEmptyXml(resultXml)) {
					resultNode = (Node) this.xpath
							.evaluate("/", new InputSource(this.resultStream), XPathConstants.NODE);
					nodeList = (NodeList) this.xprEntity.evaluate(resultNode, XPathConstants.NODESET);
					log.debug("nodeList.length: {}", nodeList.getLength());
					for (int i = 0; i < nodeList.getLength(); i++) {
						final StringWriter sw = new StringWriter();
						this.transformer.transform(new DOMSource(nodeList.item(i)), new StreamResult(sw));
						final String toEnqueue = sw.toString();
						if ((toEnqueue == null) || StringUtils.isBlank(toEnqueue) || isEmptyXml(toEnqueue)) {
							log.warn("The following record resulted in empty item for the feeding queue: {}", resultXml);
						} else {
							this.recordQueue.add(sw.toString());
						}
					}
				} else {
					log.warn("resultXml is equal with emptyXml");
				}

				this.resumptionInt += this.pageSize;

				this.resumptionStr = this.xprResumptionPath.evaluate(resultNode);

			} catch (final Exception e) {
				log.error(e.getMessage(), e);
				throw new IllegalStateException("collection failed: " + e.getMessage());
			}

			try {
				if (this.resultTotal == -1) {
					this.resultTotal = Integer.parseInt(this.xprResultTotalPath.evaluate(resultNode));
					log.info("resultTotal was -1 is now: " + this.resultTotal);
				}
			} catch (final Exception e) {
				log.error(e.getMessage(), e);
				throw new IllegalStateException("downloadPage resultTotal couldn't parse: " + e.getMessage());
			}
			log.debug("resultTotal: " + this.resultTotal);
			log.debug("resInt: " + this.resumptionInt);
			if (this.resumptionInt <= this.resultTotal) {
				nextQuery = this.baseUrl + "?filter:is_published:d=true&format=json&page[size]=" + this.pageSize + "&page="
						+ this.resumptionStr;
			} else {
				nextQuery = "";
			}
			log.debug("nextQueryUrl: " + nextQuery);
			return nextQuery;
		} catch (final Throwable e) {
			log.warn(e.getMessage(), e);
			return downloadPage(query, attempt + 1);
		}

	}

	private boolean isEmptyXml(final String s) {
		return EMPTY_XML.equalsIgnoreCase(s);
	}

}
