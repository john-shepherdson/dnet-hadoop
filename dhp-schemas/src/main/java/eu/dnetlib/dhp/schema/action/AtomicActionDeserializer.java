
package eu.dnetlib.dhp.schema.action;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import java.io.IOException;

public class AtomicActionDeserializer extends JsonDeserializer {

	@Override
	public Object deserialize(JsonParser jp, DeserializationContext ctxt)
		throws IOException, JsonProcessingException {
		JsonNode node = jp.getCodec().readTree(jp);
		String classTag = node.get("clazz").asText();
		JsonNode payload = node.get("payload");
		ObjectMapper mapper = new ObjectMapper();

		try {
			final Class<?> clazz = Class.forName(classTag);
			return new AtomicAction(clazz, (Oaf) mapper.readValue(payload.toString(), clazz));
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}
}
