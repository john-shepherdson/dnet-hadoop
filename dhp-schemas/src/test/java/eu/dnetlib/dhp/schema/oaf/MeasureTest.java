
package eu.dnetlib.dhp.schema.oaf;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

public class MeasureTest {

	public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.setSerializationInclusion(JsonInclude.Include.NON_NULL);

	@Test
	public void testMeasureSerialization() throws IOException {

		Measure popularity = new Measure();
		popularity.setId("popularity");
		popularity
			.setUnit(
				Lists
					.newArrayList(
						unit("score", "0.5")));

		Measure influence = new Measure();
		influence.setId("influence");
		influence
			.setUnit(
				Lists
					.newArrayList(
						unit("score", "0.3")));

		List<Measure> m = Lists.newArrayList(popularity, influence);

		String s = OBJECT_MAPPER.writeValueAsString(m);
		System.out.println(s);

		List<Measure> mm = OBJECT_MAPPER.readValue(s, new TypeReference<List<Measure>>() {
		});

		Assertions.assertNotNull(mm);
	}

	private KeyValue unit(String key, String value) {
		KeyValue unit = new KeyValue();
		unit.setKey(key);
		unit.setValue(value);
		return unit;
	}

}
