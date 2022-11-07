
package eu.dnetlib.dhp.actionmanager.ror.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

public class ExternalIdTypeDeserializer extends JsonDeserializer<ExternalIdType> {

	@Override
	public ExternalIdType deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException {
		final ObjectCodec oc = p.getCodec();
		final JsonNode node = oc.readTree(p);

		final JsonNode allNode = node.get("all");

		final String preferred = node.get("preferred").asText();

		final List<String> all = new ArrayList<>();

		if (allNode.isArray()) {
			allNode.elements().forEachRemaining(x -> all.add(x.asText()));
		} else {
			all.add(allNode.asText());
		}

		return new ExternalIdType(all, preferred);
	}

}
