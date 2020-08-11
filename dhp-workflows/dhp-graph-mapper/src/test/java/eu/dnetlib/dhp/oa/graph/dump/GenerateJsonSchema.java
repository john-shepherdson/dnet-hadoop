
package eu.dnetlib.dhp.oa.graph.dump;

import eu.dnetlib.dhp.schema.dump.oaf.graph.*;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.victools.jsonschema.generator.*;


public class GenerateJsonSchema {

	@Test
	public void generateSchema() {
		SchemaGeneratorConfigBuilder configBuilder = new SchemaGeneratorConfigBuilder(SchemaVersion.DRAFT_7,
			OptionPreset.PLAIN_JSON)
				.with(Option.SCHEMA_VERSION_INDICATOR)
				.without(Option.NONPUBLIC_NONSTATIC_FIELDS_WITHOUT_GETTERS);
		configBuilder.forFields().withDescriptionResolver(field -> "Description of " + field.getDeclaredName());
		SchemaGeneratorConfig config = configBuilder.build();
		SchemaGenerator generator = new SchemaGenerator(config);
		JsonNode jsonSchema = generator.generateSchema(Relation.class);

		System.out.println(jsonSchema.toString());
	}
}
