
package eu.dnetlib.dhp.collection.plugin.rest;

import java.io.InputStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.*;

import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHeaders;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import eu.dnetlib.dhp.collection.CollectorException;
import eu.dnetlib.dhp.collection.HttpClientParams;
import eu.dnetlib.dhp.collection.JsonUtils;

/**
 * log.debug(...) equal to  log.trace(...) in the application-logs
 * <p>
 * known bug: at resumptionType 'discover' if the (resultTotal % resultSizeValue) == 0 the collecting fails -> change the resultSizeValue
 *
 * @author Jochen Schirrwagen, Aenne Loehden, Andreas Czerniak
 * @date 2020-04-09
 *
 */
public class RestIterator implements Iterator<String> {

	private static final Log log = LogFactory.getLog(RestIterator.class);

	private HttpClientParams clientParams;

	private final String BASIC = "basic";

	private JsonUtils jsonUtils;

	private String baseUrl;
	private String resumptionType;
	private String resumptionParam;
	private String resultFormatValue;
	private String queryParams;
	private int resultSizeValue;
	private int resumptionInt = 0; // integer resumption token (first record to harvest)
	private int resultTotal = -1;
	private String resumptionStr = Integer.toString(resumptionInt); // string resumption token (first record to harvest
																	// or token scanned from results)
	private InputStream resultStream;
	private Transformer transformer;
	private XPath xpath;
	private String query;
	private XPathExpression xprResultTotalPath;
	private XPathExpression xprResumptionPath;
	private XPathExpression xprEntity;
	private String queryFormat;
	private String querySize;
	private String authMethod;
	private String authToken;
	private final Queue<String> recordQueue = new PriorityBlockingQueue<String>();
	private int discoverResultSize = 0;
	private int pagination = 1;

	/**
	 * RestIterator class
	 * 
	 * compatible to version before 1.3.33
	 * 
	 * @param baseUrl
	 * @param resumptionType
	 * @param resumptionParam
	 * @param resumptionXpath
	 * @param resultTotalXpath
	 * @param resultFormatParam
	 * @param resultFormatValue
	 * @param resultSizeParam
	 * @param resultSizeValueStr
	 * @param queryParams
	 * @param entityXpath
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
		final String entityXpath) {
		this(clientParams, baseUrl, resumptionType, resumptionParam, resumptionXpath, resultTotalXpath,
			resultFormatParam, resultFormatValue, resultSizeParam, resultSizeValueStr, queryParams, entityXpath, "",
			"");
	}

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
		final String resultOffsetParam) {
		this(clientParams, baseUrl, resumptionType, resumptionParam, resumptionXpath, resultTotalXpath,
			resultFormatParam, resultFormatValue, resultSizeParam, resultSizeValueStr, queryParams, entityXpath, "",
			"");
	}

	/** RestIterator class
	 *  compatible to version 1.3.33
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
		final String authToken) {
		this.clientParams = clientParams;
		this.jsonUtils = new JsonUtils();
		this.baseUrl = baseUrl;
		this.resumptionType = resumptionType;
		this.resumptionParam = resumptionParam;
		this.resultFormatValue = resultFormatValue;
		this.queryParams = queryParams;
		this.resultSizeValue = Integer.valueOf(resultSizeValueStr);
		this.authMethod = authMethod;
		this.authToken = authToken;

		queryFormat = StringUtils.isNotBlank(resultFormatParam) ? "&" + resultFormatParam + "=" + resultFormatValue
			: "";
		querySize = StringUtils.isNotBlank(resultSizeParam) ? "&" + resultSizeParam + "=" + resultSizeValueStr : "";

		try {
			initXmlTransformation(resultTotalXpath, resumptionXpath, entityXpath);
		} catch (Exception e) {
			throw new IllegalStateException("xml transformation init failed: " + e.getMessage());
		}
		initQueue();
	}

	private void initXmlTransformation(String resultTotalXpath, String resumptionXpath, String entityXpath)
		throws TransformerConfigurationException, XPathExpressionException {
		transformer = TransformerFactory.newInstance().newTransformer();
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "3");
		xpath = XPathFactory.newInstance().newXPath();
		xprResultTotalPath = xpath.compile(resultTotalXpath);
		xprResumptionPath = xpath.compile(StringUtils.isBlank(resumptionXpath) ? "/" : resumptionXpath);
		xprEntity = xpath.compile(entityXpath);
	}

	private void initQueue() {
		query = baseUrl + "?" + queryParams + querySize + queryFormat;
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
		if (recordQueue.isEmpty() && query.isEmpty()) {
			disconnect();
			return false;
		} else {
			return true;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public String next() {
		synchronized (recordQueue) {
			while (recordQueue.isEmpty() && !query.isEmpty()) {
				try {
					log.debug("get Query: " + query);
					query = downloadPage(query);
					log.debug("next queryURL from downloadPage(): " + query);
				} catch (CollectorException e) {
					log.debug("CollectorPlugin.next()-Exception: " + e);
					throw new RuntimeException(e);
				}
			}
			return recordQueue.poll();
		}
	}

	/*
	 * download page and return nextQuery
	 */
	private String downloadPage(String query) throws CollectorException {
		String resultJson;
		String resultXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
		String emptyXml = resultXml + "<" + JsonUtils.wrapName + "></" + JsonUtils.wrapName + ">";
		Node resultNode = null;
		NodeList nodeList = null;
		InputStream theHttpInputStream;

		// check if cursor=* is initial set otherwise add it to the queryParam URL
		if (resumptionType.equalsIgnoreCase("deep-cursor")) {
			log.debug("check resumptionType deep-cursor and check cursor=*?" + query);
			if (!query.contains("&cursor=")) {
				query += "&cursor=*";
			}
		}

		try {
			URL qUrl = new URL(query);
			log.debug("authMethod :" + authMethod);
			if (this.authMethod == "bearer") {
				log.trace("authMethod before inputStream: " + resultXml);
				HttpURLConnection conn = (HttpURLConnection) qUrl.openConnection();
				conn.setRequestProperty(HttpHeaders.AUTHORIZATION, "Bearer " + authToken);
				conn.setRequestProperty(HttpHeaders.CONTENT_TYPE, "application/json");
				conn.setRequestMethod("GET");
				theHttpInputStream = conn.getInputStream();
			} else if (BASIC.equalsIgnoreCase(this.authMethod)) {
				log.trace("authMethod before inputStream: " + resultXml);
				HttpURLConnection conn = (HttpURLConnection) qUrl.openConnection();
				conn.setRequestProperty(HttpHeaders.AUTHORIZATION, "Basic " + authToken);
				conn.setRequestProperty(HttpHeaders.ACCEPT, "application/xml");
				conn.setRequestMethod("GET");
				theHttpInputStream = conn.getInputStream();
			} else {
				theHttpInputStream = qUrl.openStream();
			}

			resultStream = theHttpInputStream;
			if ("json".equalsIgnoreCase(resultFormatValue)) {
				resultJson = IOUtils.toString(resultStream, "UTF-8");
				resultXml = jsonUtils.convertToXML(resultJson);
				resultStream = IOUtils.toInputStream(resultXml, "UTF-8");
			}

			if (!(emptyXml).equalsIgnoreCase(resultXml)) {
				resultNode = (Node) xpath.evaluate("/", new InputSource(resultStream), XPathConstants.NODE);
				nodeList = (NodeList) xprEntity.evaluate(resultNode, XPathConstants.NODESET);
				log.debug("nodeList.length: " + nodeList.getLength());
				for (int i = 0; i < nodeList.getLength(); i++) {
					StringWriter sw = new StringWriter();
					transformer.transform(new DOMSource(nodeList.item(i)), new StreamResult(sw));
					recordQueue.add(sw.toString());
				}
			} else {
				log.info("resultXml is equal with emptyXml");
			}

			resumptionInt += resultSizeValue;

			String qUrlArgument = "";
			switch (resumptionType.toLowerCase()) {
				case "scan": // read of resumptionToken , evaluate next results, e.g. OAI, iterate over items
					resumptionStr = xprResumptionPath.evaluate(resultNode);
					break;

				case "count": // begin at one step for all records, iterate over items
					resumptionStr = Integer.toString(resumptionInt);
					break;

				case "discover": // size of result items unknown, iterate over items (for openDOAR - 201808)
					if (resultSizeValue < 2) {
						throw new CollectorException("Mode: discover, Param 'resultSizeValue' is less than 2");
					}
					qUrlArgument = qUrl.getQuery();
					String[] arrayQUrlArgument = qUrlArgument.split("&");
					int urlOldResumptionSize = 0;
					for (String arrayUrlArgStr : arrayQUrlArgument) {
						if (arrayUrlArgStr.startsWith(resumptionParam)) {
							String[] resumptionKeyValue = arrayUrlArgStr.split("=");
							if (isInteger(resumptionKeyValue[1])) {
								urlOldResumptionSize = Integer.parseInt(resumptionKeyValue[1]);
								log.debug("discover OldResumptionSize from Url (int): " + urlOldResumptionSize);
							} else {
								log.debug("discover OldResumptionSize from Url (str): " + resumptionKeyValue[1]);
							}
						}
					}

					if (((emptyXml).equalsIgnoreCase(resultXml))
						|| ((nodeList != null) && (nodeList.getLength() < resultSizeValue))) {
						// resumptionStr = "";
						if (nodeList != null) {
							discoverResultSize += nodeList.getLength();
						}
						resultTotal = discoverResultSize;
					} else {
						resumptionStr = Integer.toString(resumptionInt);
						resultTotal = resumptionInt + 1;
						if (nodeList != null) {
							discoverResultSize += nodeList.getLength();
						}
					}
					log.debug("discoverResultSize:  " + discoverResultSize);
					break;

				case "pagination":
				case "page": // pagination, iterate over page numbers
					pagination += 1;
					if (nodeList != null) {
						discoverResultSize += nodeList.getLength();
					} else {
						resultTotal = discoverResultSize;
						pagination = discoverResultSize;
					}
					resumptionInt = pagination;
					resumptionStr = Integer.toString(resumptionInt);
					break;

				case "deep-cursor": // size of result items unknown, iterate over items (for supporting deep cursor in
									// solr)
					// isn't relevant -- if (resultSizeValue < 2) {throw new CollectorServiceException("Mode:
					// deep-cursor, Param 'resultSizeValue' is less than 2");}

					resumptionStr = encodeValue(xprResumptionPath.evaluate(resultNode));
					queryParams = queryParams.replace("&cursor=*", "");

					// terminating if length of nodeList is 0
					if ((nodeList != null) && (nodeList.getLength() < discoverResultSize)) {
						resumptionInt += (nodeList.getLength() + 1 - resultSizeValue);
					} else {
						resumptionInt += (nodeList.getLength() - resultSizeValue); // subtract the resultSizeValue
																					// because the iteration is over
																					// real length and the
																					// resultSizeValue is added before
																					// the switch()
					}

					discoverResultSize = nodeList.getLength();

					log
						.debug(
							"downloadPage().deep-cursor: resumptionStr=" + resumptionStr + " ; queryParams="
								+ queryParams + " resumptionLengthIncreased: " + resumptionInt);

					break;

				default: // otherwise: abort
					// resultTotal = resumptionInt;
					break;
			}

		} catch (Exception e) {
			log.error(e);
			throw new IllegalStateException("collection failed: " + e.getMessage());
		}

		try {
			if (resultTotal == -1) {
				resultTotal = Integer.parseInt(xprResultTotalPath.evaluate(resultNode));
				if (resumptionType.toLowerCase().equals("page") && !BASIC.equalsIgnoreCase(authMethod)) {
					resultTotal += 1;
				} // to correct the upper bound
				log.info("resultTotal was -1 is now: " + resultTotal);
			}
		} catch (Exception e) {
			log.error(e);
			throw new IllegalStateException("downloadPage resultTotal couldn't parse: " + e.getMessage());
		}
		log.debug("resultTotal: " + resultTotal);
		log.debug("resInt: " + resumptionInt);
		String nextQuery;
		if (resumptionInt <= resultTotal) {
			nextQuery = baseUrl + "?" + queryParams + querySize + "&" + resumptionParam + "=" + resumptionStr
				+ queryFormat;
		} else {
			nextQuery = "";
			// if (resumptionType.toLowerCase().equals("deep-cursor")) { resumptionInt -= 1; } // correct the
			// resumptionInt and prevent a NullPointer Exception at mdStore
		}
		log.debug("nextQueryUrl: " + nextQuery);
		return nextQuery;
	}

	private boolean isInteger(String s) {
		boolean isValidInteger = false;
		try {
			Integer.parseInt(s);

			// s is a valid integer

			isValidInteger = true;
		} catch (NumberFormatException ex) {
			// s is not an integer
		}

		return isValidInteger;
	}

	// Method to encode a string value using `UTF-8` encoding scheme
	private String encodeValue(String value) {
		try {
			return URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
		} catch (UnsupportedEncodingException ex) {
			throw new RuntimeException(ex.getCause());
		}
	}

}
