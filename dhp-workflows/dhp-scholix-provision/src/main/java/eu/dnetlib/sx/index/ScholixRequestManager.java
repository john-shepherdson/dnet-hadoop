
package eu.dnetlib.sx.index;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

public class ScholixRequestManager extends RequestManager {

	final static String identifierPath = "$.identifier";

	@Override
	public IndexRequest createRequest(final String line, final String indexName) {
		return new IndexRequest()
			.index(indexName)
			.id(extractIdentifier(line, identifierPath))
			.source(line, XContentType.JSON);

	}
}
