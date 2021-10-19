
package eu.dnetlib.dhp.actionmanager.ror;

import java.io.FileInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.ror.model.RorOrganization;
import eu.dnetlib.dhp.schema.oaf.Organization;

@Disabled
class GenerateRorActionSetJobTest {

	private static final ObjectMapper mapper = new ObjectMapper();

	private static final String local_file_path = "/Users/michele/Downloads/ror-data-2021-04-06.json";

	@BeforeEach
	void setUp() throws Exception {
	}

	@Test
	void testConvertRorOrg() throws Exception {
		final RorOrganization r = mapper
			.readValue(IOUtils.toString(getClass().getResourceAsStream("ror_org.json")), RorOrganization.class);
		final Organization org = GenerateRorActionSetJob.convertRorOrg(r);

		final String s = mapper.writeValueAsString(org);
		Assertions.assertTrue(StringUtils.isNotBlank(s));
		System.out.println(s);
	}

	@Test
	void testConvertAllRorOrg() throws Exception {
		final RorOrganization[] arr = mapper
			.readValue(IOUtils.toString(new FileInputStream(local_file_path)), RorOrganization[].class);

		for (final RorOrganization r : arr) {
			Organization o = GenerateRorActionSetJob.convertRorOrg(r);
			Assertions.assertNotNull(o);
			Assertions.assertTrue(StringUtils.isNotBlank(o.getId()));
		}
	}

}
