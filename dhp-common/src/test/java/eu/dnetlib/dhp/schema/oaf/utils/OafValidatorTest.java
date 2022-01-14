
package eu.dnetlib.dhp.schema.oaf.utils;

import java.io.IOException;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Publication;

import static org.junit.jupiter.api.Assertions.*;

public class OafValidatorTest {

	@Test
	void test_validate() throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		String json = IOUtils.toString(getClass().getResourceAsStream("publication_3.json"));

		Publication p = mapper.readValue(json, Publication.class);

		Set<ConstraintViolation> res = OafValidator.validate(p);

		assertNotNull(res);
		assertFalse(res.isEmpty());

		res.forEach(v -> {
			System.out.println(v.getProperties());
		});

		String reportJson = mapper.writeValueAsString(res);
		assertNotNull(reportJson);

		Set<ConstraintViolation> report = mapper.readValue(reportJson, new TypeReference<Set<ConstraintViolation>>() { });

		assertNotNull(report);

		System.out.println(mapper.writeValueAsString(report));
	}

}
