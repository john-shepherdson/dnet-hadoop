
package eu.dnetlib.sx.index;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SummaryRequestManager extends RequestManager {

	final static String identifierPath = "$.id";
	final static ObjectMapper mapper = new ObjectMapper();

	private String constructSource(String line) {
		Map<String, Object> params = new HashMap<>();
		params.put("body", line);
		try {
			return mapper.writeValueAsString(params);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public IndexRequest createRequest(String line, String indexName) {
		return new IndexRequest()
			.index(indexName)
			.id(extractIdentifier(line, identifierPath))
			.source(constructSource(line), XContentType.JSON);
	}
}
