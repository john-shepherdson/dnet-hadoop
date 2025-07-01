package eu.dnetlib.dhp.index.es;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.util.BinaryData;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * ConvertJSONWithId is a utility class that converts a JSON string into a BulkOperation
 * for Elasticsearch, extracting the document ID using a specified regular expression.
 * It is used to index documents with a specific ID in Elasticsearch.
 *
 * <p>Usage example:</p>
 * <pre>
 *   ConvertJSONWithId converter = new ConvertJSONWithId("\\\"id\\\":\\s*\\\"(.*?)\\\"", "my-index");
 *   BulkOperation operation = converter.apply("{\"id\": \"123\", \"name\": \"example\"}");
 * </pre>
 */
public class ConvertJSONWithId  implements Function<String, BulkOperation> {
    private final Pattern idRegEx;
    private final String indexName;

    private String search(String text, Pattern pattern) {

        final Matcher matcher = pattern.matcher(text);

        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    public ConvertJSONWithId(String regxId, String indexName) {
        this.idRegEx= Pattern.compile(regxId);
        this.indexName= indexName;
    }

    @Override
    public BulkOperation apply(String json) {
        String id = search(json, this.idRegEx);
        return new BulkOperation.Builder()
                .index(
                        i -> i
                                .index(this.indexName)
                                .id(id)
                                .document(BinaryData.of(json.getBytes(StandardCharsets.UTF_8), "application/json")))
                .build();
    }

}
