/**
 * 
 */

package eu.dnetlib.dhp.collection.plugin.rest;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;

/**
 * 
 * @author js, Andreas Czerniak
 * @date 2020-04-08
 */
public class RestIteratorTest {

	private static final Logger log = LoggerFactory.getLogger(RestIteratorTest.class);

	private final String baseUrl = "https://share.osf.io/api/v2/search/creativeworks/_search";
	private final String resumptionType = "count";
	private final String resumptionParam = "from";
	private final String resumptionXpath = "";
	private final String resultTotalXpath = "//hits/total";
	private final String entityXpath = "//hits/hits";
	private final String resultFormatParam = "format";
	private final String resultFormatValue = "Json"; // Change from lowerCase to one UpperCase
	private final String resultSizeParam = "size";
	private final String resultSizeValue = "10";
	private final String authMethod = "";
	private final String authToken = "";
	private final String resultOffsetParam = "cursor";
	private final String query = "q=%28sources%3ASocArXiv+AND+type%3Apreprint%29";

	@Disabled
	@Test
	public void test() throws CollectorException {

		HttpClientParams clientParams = new HttpClientParams();

		final RestIterator iterator = new RestIterator(clientParams, baseUrl, resumptionType, resumptionParam,
			resumptionXpath, resultTotalXpath, resultFormatParam, resultFormatValue, resultSizeParam, resultSizeValue,
			query, entityXpath, authMethod, authToken, resultOffsetParam);
		int i = 20;
		while (iterator.hasNext() && i > 0) {
			String result = iterator.next();

			i--;
		}
	}
}
