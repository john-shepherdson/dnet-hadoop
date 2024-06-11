
package eu.dnetlib.dhp.collection.plugin.rest;

import java.io.InputStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.*;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.common.collect.Maps;

import eu.dnetlib.dhp.collection.plugin.utils.JsonUtils;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;

/**
 * log.info(...) equal to log.trace(...) in the application-logs
 * <p>
 * known bug: at resumptionType 'discover' if the (resultTotal % resultSizeValue) == 0 the collecting fails -> change the resultSizeValue
 *
 * @author Jochen Schirrwagen, Aenne Loehden, Andreas Czerniak
 * @date 2020-04-09
 *
 */
public class RestIterator implements Iterator<String> {
	private static final Logger log = LoggerFactory.getLogger(RestIterator.class);
	public static final String UTF_8 = "UTF-8";
	private static final int MAX_ATTEMPTS = 5;

	private final HttpClientParams clientParams;

	private final String AUTHBASIC = "basic";

	private static final String XML_HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
	private static final String EMPTY_XML = XML_HEADER + "<" + JsonUtils.XML_WRAP_TAG + "></" + JsonUtils.XML_WRAP_TAG
		+ ">";

	private final String baseUrl;
	private final String resumptionType;
	private final String resumptionParam;
	private final String resultFormatValue;
	private String queryParams = "";
	private final int resultSizeValue;
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
	private final String queryFormat;
	private final String querySize;
	private final String authMethod;
	private final String authToken;
	private final Queue<String> recordQueue = new PriorityBlockingQueue<>();
	private int discoverResultSize = 0;
	private int pagination = 1;
	/*
	 * While resultFormatValue is added to the request parameter, this is used to say that the results are retrieved in
	 * json. useful for cases when the target API expects a resultFormatValue != json, but the results are returned in
	 * json. An example is the EU Open Data Portal API: resultFormatValue=standard, results are in json format.
	 */
	private final String resultOutputFormat;

	/*
	 * Can be used to set additional request headers, like for content negotiation
	 */
	private Map<String, String> requestHeaders;

	/**
	 * RestIterator class compatible to version 1.3.33
	 */
	public RestIterator(
		final HttpClientParams clientParams,
		final String baseUrl,
		final String resumptionType,
		final String resumptionParam,
		final String resumptionXpath,
		final String resultTotalXpath,
		final String resultFormatParam,
		final String resultFormatValue,
		final String resultSizeParam,
		final String resultSizeValueStr,
		final String queryParams,
		final String entityXpath,
		final String authMethod,
		final String authToken,
		final String resultOutputFormat,
		final Map<String, String> requestHeaders) {

		this.clientParams = clientParams;
		this.baseUrl = baseUrl;
		this.resumptionType = resumptionType;
		this.resumptionParam = resumptionParam;
		this.resultFormatValue = resultFormatValue;
		this.resultSizeValue = Integer.parseInt(resultSizeValueStr);
		this.queryParams = queryParams;
		this.authMethod = authMethod;
		this.authToken = authToken;
		this.resultOutputFormat = resultOutputFormat;
		this.requestHeaders = requestHeaders != null ? requestHeaders : Maps.newHashMap();

		this.queryFormat = StringUtils.isNotBlank(resultFormatParam) ? "&" + resultFormatParam + "=" + resultFormatValue
			: "";
		this.querySize = StringUtils.isNotBlank(resultSizeParam) ? "&" + resultSizeParam + "=" + resultSizeValueStr
			: "";

		try {
			initXmlTransformation(resultTotalXpath, resumptionXpath, entityXpath);
		} catch (final Exception e) {
			throw new IllegalStateException("xml transformation init failed: " + e.getMessage());
		}

		initQueue();
	}

	private void initXmlTransformation(final String resultTotalXpath, final String resumptionXpath,
		final String entityXpath)
		throws TransformerConfigurationException, XPathExpressionException {
		final TransformerFactory factory = TransformerFactory.newInstance();
		this.transformer = factory.newTransformer();
		this.transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		this.transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "3");
		this.xpath = XPathFactory.newInstance().newXPath();
		this.xprResultTotalPath = this.xpath.compile(resultTotalXpath);
		this.xprResumptionPath = this.xpath.compile(StringUtils.isBlank(resumptionXpath) ? "/" : resumptionXpath);
		this.xprEntity = this.xpath.compile(entityXpath);
	}

	private void initQueue() {
		if (queryParams.equals("") && querySize.equals("") && queryFormat.equals("")) {
			query = baseUrl;
		} else {
			query = baseUrl + "?" + queryParams + querySize + queryFormat;
		}

		log.info("REST calls starting with {}", this.query);
	}

	private void disconnect() {
		// TODO close inputstream
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		if (this.recordQueue.isEmpty() && this.query.isEmpty()) {
			disconnect();
			return false;
		}
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public String next() {
		synchronized (this.recordQueue) {
			while (this.recordQueue.isEmpty() && !this.query.isEmpty()) {
				try {
					this.query = downloadPage(this.query, 0);
				} catch (final CollectorException e) {
					log.debug("CollectorPlugin.next()-Exception: {}", e);
					throw new RuntimeException(e);
				}
			}
			return this.recordQueue.poll();
		}
	}

	/*
	 * download page and return nextQuery (with number of attempt)
	 */
	private String downloadPage(String query, final int attempt) throws CollectorException {

		if (attempt > MAX_ATTEMPTS) {
			throw new CollectorException("Max Number of attempts reached, query:" + query);
		}

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
			String qUrlArgument = "";
			int urlOldResumptionSize = 0;
			InputStream theHttpInputStream;

			// check if cursor=* is initial set otherwise add it to the queryParam URL
			if ("deep-cursor".equalsIgnoreCase(this.resumptionType)) {
				log.debug("check resumptionType deep-cursor and check cursor=*?{}", query);
				if (!query.contains("&cursor=")) {
					query += "&cursor=*";
				}
			}

			// find pagination page start number in queryParam and remove before start the first query
			if ((resumptionType.toLowerCase().equals("pagination") || resumptionType.toLowerCase().equals("page"))
				&& (query.contains("paginationStart="))) {

				final Matcher m = Pattern.compile("paginationStart=([0-9]+)").matcher(query);
				m.find(); // guaranteed to be true for this regex

				String[] pageVal = m.group(0).split("=");
				pagination = Integer.parseInt(pageVal[1]);

				// remove page start number from query and queryParams
				queryParams = queryParams.replaceFirst("&?paginationStart=[0-9]+", "");
				query = query.replaceFirst("&?paginationStart=[0-9]+", "");

			}

			try {
				log.info("requesting URL [{}]", query);

				final URL qUrl = new URL(query);
				log.debug("authMethod: {}", this.authMethod);
				if (this.authMethod == "bearer") {
					log.trace("RestIterator.downloadPage():: authMethod before inputStream: " + resultXml);
					requestHeaders.put("Authorization", "Bearer " + authToken);
					// requestHeaders.put("Content-Type", "application/json");
				} else if (AUTHBASIC.equalsIgnoreCase(this.authMethod)) {
					log.trace("RestIterator.downloadPage():: authMethod before inputStream: " + resultXml);
					requestHeaders.put("Authorization", "Basic " + authToken);
					// requestHeaders.put("accept", "application/xml");
				}
				HttpURLConnection conn = (HttpURLConnection) qUrl.openConnection();
				conn.setRequestMethod("GET");
				this.setRequestHeader(conn);
				resultStream = conn.getInputStream();

				if ("json".equals(this.resultOutputFormat)) {
					resultJson = IOUtils.toString(this.resultStream, StandardCharsets.UTF_8);
					resultXml = JsonUtils.convertToXML(resultJson);
					this.resultStream = IOUtils.toInputStream(resultXml, UTF_8);
				}

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
							log
								.warn(
									"The following record resulted in empty item for the feeding queue: {}", resultXml);
						} else {
							this.recordQueue.add(sw.toString());
						}
					}
				} else {
					log.warn("resultXml is equal with emptyXml");
				}

				this.resumptionInt += this.resultSizeValue;

				switch (this.resumptionType.toLowerCase()) {
					case "scan": // read of resumptionToken , evaluate next results, e.g. OAI, iterate over items
						this.resumptionStr = this.xprResumptionPath.evaluate(resultNode);
						break;

					case "count": // begin at one step for all records, iterate over items
						this.resumptionStr = Integer.toString(this.resumptionInt);
						break;

					case "discover": // size of result items unknown, iterate over items (for openDOAR - 201808)
						if (this.resultSizeValue < 2) {
							throw new CollectorException("Mode: discover, Param 'resultSizeValue' is less than 2");
						}
						qUrlArgument = qUrl.getQuery();

						final String[] arrayQUrlArgument = qUrlArgument.split("&");
						for (final String arrayUrlArgStr : arrayQUrlArgument) {
							if (arrayUrlArgStr.startsWith(this.resumptionParam)) {
								final String[] resumptionKeyValue = arrayUrlArgStr.split("=");
								if (isInteger(resumptionKeyValue[1])) {
									urlOldResumptionSize = Integer.parseInt(resumptionKeyValue[1]);
									log.debug("discover OldResumptionSize from Url (int): {}", urlOldResumptionSize);
								} else {
									log.debug("discover OldResumptionSize from Url (str): {}", resumptionKeyValue[1]);
								}
							}
						}

						if (isEmptyXml(resultXml)
							|| ((nodeList != null) && (nodeList.getLength() < this.resultSizeValue))) {
							// resumptionStr = "";
							if (nodeList != null) {
								this.discoverResultSize += nodeList.getLength();
							}
							this.resultTotal = this.discoverResultSize;
						} else {
							this.resumptionStr = Integer.toString(this.resumptionInt);
							this.resultTotal = this.resumptionInt + 1;
							if (nodeList != null) {
								this.discoverResultSize += nodeList.getLength();
							}
						}
						log.info("discoverResultSize: {}", this.discoverResultSize);
						break;

					case "pagination":
					case "page": // pagination, iterate over page numbers
						if (nodeList != null && nodeList.getLength() > 0) {
							this.discoverResultSize += nodeList.getLength();
						} else {
							this.resultTotal = this.discoverResultSize;
							this.pagination = this.discoverResultSize;
						}
						this.pagination += 1;
						this.resumptionInt = this.pagination;
						this.resumptionStr = Integer.toString(this.resumptionInt);
						break;

					case "deep-cursor": // size of result items unknown, iterate over items (for supporting deep cursor
										// in
										// solr)
						// isn't relevant -- if (resultSizeValue < 2) {throw new CollectorServiceException("Mode:
						// deep-cursor, Param 'resultSizeValue' is less than 2");}

						this.resumptionStr = encodeValue(this.xprResumptionPath.evaluate(resultNode));
						this.queryParams = this.queryParams.replace("&cursor=*", "");

						// terminating if length of nodeList is 0
						if ((nodeList != null) && (nodeList.getLength() < this.discoverResultSize)) {
							this.resumptionInt += ((nodeList.getLength() + 1) - this.resultSizeValue);
						} else {
							this.resumptionInt += (nodeList.getLength() - this.resultSizeValue); // subtract the
																									// resultSizeValue
							// because the iteration is over
							// real length and the
							// resultSizeValue is added before
							// the switch()
						}

						this.discoverResultSize = nodeList.getLength();

						log
							.debug(
								"downloadPage().deep-cursor: resumptionStr=" + this.resumptionStr + " ; queryParams="
									+ this.queryParams + " resumptionLengthIncreased: " + this.resumptionInt);

						break;

					default: // otherwise: abort
						// resultTotal = resumptionInt;
						break;
				}

			} catch (final Exception e) {
				log.error(e.getMessage(), e);
				throw new IllegalStateException("collection failed: " + e.getMessage());
			}

			try {
				if (this.resultTotal == -1) {
					this.resultTotal = Integer.parseInt(this.xprResultTotalPath.evaluate(resultNode));
					if ("page".equalsIgnoreCase(this.resumptionType)
						&& !this.AUTHBASIC.equalsIgnoreCase(this.authMethod)) {
						this.resultTotal += 1;
					} // to correct the upper bound
					log.info("resultTotal was -1 is now: " + this.resultTotal);
				}
			} catch (final Exception e) {
				log.error(e.getMessage(), e);
				throw new IllegalStateException("downloadPage resultTotal couldn't parse: " + e.getMessage());
			}
			log.debug("resultTotal: " + this.resultTotal);
			log.debug("resInt: " + this.resumptionInt);
			if (this.resumptionInt <= this.resultTotal) {
				nextQuery = this.baseUrl + "?" + this.queryParams + this.querySize + "&" + this.resumptionParam + "="
					+ this.resumptionStr
					+ this.queryFormat;
			} else {
				nextQuery = "";
				// if (resumptionType.toLowerCase().equals("deep-cursor")) { resumptionInt -= 1; } // correct the
				// resumptionInt and prevent a NullPointer Exception at mdStore
			}
			log.debug("nextQueryUrl: " + nextQuery);
			return nextQuery;
		} catch (final Throwable e) {
			log.warn(e.getMessage(), e);
			return downloadPage(query, attempt + 1);
		}

	}

	private boolean isEmptyXml(String s) {
		return EMPTY_XML.equalsIgnoreCase(s);
	}

	private boolean isInteger(final String s) {
		boolean isValidInteger = false;
		try {
			Integer.parseInt(s);

			// s is a valid integer

			isValidInteger = true;
		} catch (final NumberFormatException ex) {
			// s is not an integer
		}

		return isValidInteger;
	}

	// Method to encode a string value using `UTF-8` encoding scheme
	private String encodeValue(final String value) {
		try {
			return URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
		} catch (final UnsupportedEncodingException ex) {
			throw new RuntimeException(ex.getCause());
		}
	}

	/**
	 * setRequestHeader
	 *
	 * setRequestProperty: Sets the general request property. If a property with the key already exists, overwrite its value with the new value.
	 * @param conn
	 */
	private void setRequestHeader(HttpURLConnection conn) {
		if (requestHeaders != null) {
			for (String key : requestHeaders.keySet()) {
				conn.setRequestProperty(key, requestHeaders.get(key));
			}
			log.debug("Set Request Header with: " + requestHeaders);
		}

	}

	public String getResultFormatValue() {
		return this.resultFormatValue;
	}

	public String getResultOutputFormat() {
		return this.resultOutputFormat;
	}

}
