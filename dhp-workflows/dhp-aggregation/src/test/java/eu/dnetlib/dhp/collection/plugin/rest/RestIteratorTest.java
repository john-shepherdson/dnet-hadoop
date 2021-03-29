/**
 * 
 */

package eu.dnetlib.dhp.collection.plugin.rest;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.collection.HttpClientParams;

/**
 * 
 * @author js, Andreas Czerniak
 * @date 2020-04-08
 */
public class RestIteratorTest {

	private static final Logger log = LoggerFactory.getLogger(RestIteratorTest.class);

	private String baseUrl = "https://share.osf.io/api/v2/search/creativeworks/_search";
	private String resumptionType = "count";
	private String resumptionParam = "from";
	private String resumptionXpath = "";
	private String resultTotalXpath = "//hits/total";
	private String entityXpath = "//hits/hits";
	private String resultFormatParam = "format";
	private String resultFormatValue = "Json"; // Change from lowerCase to one UpperCase
	private String resultSizeParam = "size";
	private String resultSizeValue = "10";
	private String authMethod = "";
	private String authToken = "";
	private String resultOffsetParam = "cursor";
	private String query = "q=%28sources%3ASocArXiv+AND+type%3Apreprint%29";

	@Disabled
	@Test
	public void test() {

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
