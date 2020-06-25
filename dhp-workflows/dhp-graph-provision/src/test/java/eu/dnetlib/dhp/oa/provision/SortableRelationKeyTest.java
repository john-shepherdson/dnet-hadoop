
package eu.dnetlib.dhp.oa.provision;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.oa.provision.model.SortableRelationKey;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class SortableRelationKeyTest {

	@Test
	public void doTesSorting() throws IOException {
		final ObjectMapper mapper = new ObjectMapper();
		final String json = IOUtils.toString(this.getClass().getResourceAsStream("relations.json"));
		final List<Relation> relations = mapper.readValue(json, new TypeReference<List<Relation>>() {
		});

		relations
			.stream()
			.map(r -> SortableRelationKey.create(r, r.getSource()))
			.sorted()
			.forEach(

				it -> {
					try {
						System.out.println(mapper.writeValueAsString(it));
					} catch (JsonProcessingException e) {
						e.printStackTrace();
					}
				});

	}

}
