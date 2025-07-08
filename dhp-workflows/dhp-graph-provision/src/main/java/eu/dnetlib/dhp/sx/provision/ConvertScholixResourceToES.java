package eu.dnetlib.dhp.sx.provision;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.util.BinaryData;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConvertScholixResourceToES implements Function<String, BulkOperation> {
    private static final Pattern summaryIDPattern = Pattern.compile("\"dnetIdentifier\":\"((\\d|\\w)*)\"");
    private static final Pattern summaryTypePattern = Pattern.compile("\"objectType\":\"((\\d|\\w)*)\"");
    private  final String  index;


    private static final ObjectMapper MAPPER = new ObjectMapper();

    public ConvertScholixResourceToES(String index) {
        this.index = index;
    }

    private String search(String text, Pattern pattern) {

        final Matcher matcher = pattern.matcher(text);

        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }




    @Override
    public BulkOperation apply(String rawJson) {
        try {
            final String dnetIdentifier = search(rawJson, summaryIDPattern);
            final String objectType = search(rawJson, summaryTypePattern);

            Map<String, String> d = new HashMap<>();
            d.put("objectType", objectType);
            d.put("body", rawJson);
            String data = MAPPER.writeValueAsString(d);

            BulkOperation result;
            result = new BulkOperation.Builder()
                    .index(
                            i -> i
                                    .index(index)
                                    .id(dnetIdentifier)
                                    .document(BinaryData.of(data.getBytes(StandardCharsets.UTF_8), "application/json")))
                    .build();

            return result;
        } catch (Throwable e) {
            throw new RuntimeException("Error processing JSON: " + rawJson, e);
        }
    }
}
