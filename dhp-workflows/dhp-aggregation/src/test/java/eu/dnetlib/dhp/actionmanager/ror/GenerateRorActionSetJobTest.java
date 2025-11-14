
package eu.dnetlib.dhp.actionmanager.ror;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.FileInputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.actionmanager.ror.model.RorOrganization;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.common.ModelConstants;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Relation;

class GenerateRorActionSetJobTest {

	private static final ObjectMapper mapper = new ObjectMapper();

	private static final String local_file_path = "/Users/michele/Downloads/ror-data-2021-04-06.json";

	@BeforeEach
	void setUp() throws Exception {}

	@Test
	void testConvertRorOrg() throws Exception {
		final RorOrganization r = mapper
				.readValue(IOUtils.toString(getClass().getResourceAsStream("ror_org.json")), RorOrganization.class);
		final List<AtomicAction<? extends Oaf>> aas = GenerateRorActionSetJob.convertRorOrg(r, false);

		Assertions.assertEquals(1, aas.size());
		assertEquals(Organization.class, aas.get(0).getClazz());

		final Organization o = (Organization) aas.get(0).getPayload();

		assertNotNull(o);

		assertNotNull(o.getCountry());
		assertEquals("AU", o.getCountry().getClassid());

		assertNotNull(o.getLegalname());
		assertEquals("Mount Stromlo Observatory", o.getLegalname().getValue());

		System.out.println(mapper.writeValueAsString(o));
	}

	@Test
	void testConvertRorOrgWirthRels() throws Exception {
		final RorOrganization r = mapper
				.readValue(IOUtils.toString(getClass().getResourceAsStream("ror_org.json")), RorOrganization.class);
		final List<AtomicAction<? extends Oaf>> aas = GenerateRorActionSetJob.convertRorOrg(r, true);

		Assertions.assertEquals(2, aas.size());
		assertEquals(Organization.class, aas.get(0).getClazz());
		assertEquals(Relation.class, aas.get(1).getClazz());

		final Organization o = (Organization) aas.get(0).getPayload();
		final Relation rel = (Relation) aas.get(1).getPayload();

		assertNotNull(o);
		assertNotNull(rel);

		assertEquals(o.getId(), rel.getSource());
		assertNotNull(rel.getTarget());

		assertEquals(ModelConstants.IS_PARENT_OF, rel.getRelClass());
		assertEquals(ModelConstants.ORG_ORG_RELTYPE, rel.getRelType());

		System.out.println(mapper.writeValueAsString(rel));

	}

	@Test
	@Disabled
	void testConvertAllRorOrg() throws Exception {
		final RorOrganization[] arr = mapper
				.readValue(IOUtils.toString(new FileInputStream(local_file_path)), RorOrganization[].class);

		for (final RorOrganization r : arr) {
			final List<AtomicAction<? extends Oaf>> aas = GenerateRorActionSetJob.convertRorOrg(r, false);
			Assertions.assertFalse(aas.isEmpty());
			Assertions.assertNotNull(aas.get(0));
			final Organization o = (Organization) aas.get(0).getPayload();
			Assertions.assertTrue(StringUtils.isNotBlank(o.getId()));
		}
	}

}
