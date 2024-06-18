
package eu.dnetlib.sx.index;

import org.elasticsearch.action.index.IndexRequest;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

public abstract class RequestManager {

	String extractIdentifier(final String json, final String jpath) {
		DocumentContext jsonContext = JsonPath.parse(json);
		return jsonContext.read(jpath);
	}

	public abstract IndexRequest createRequest(final String line, final String indexName);
}
