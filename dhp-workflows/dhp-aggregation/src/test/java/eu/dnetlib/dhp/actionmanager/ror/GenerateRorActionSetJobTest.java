
package eu.dnetlib.dhp.actionmanager.ror;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
	void setUp() throws Exception {
	}

	@Test
	void testConvertRorOrg() throws Exception {
		final RorOrganization r = mapper
			.readValue(IOUtils.toString(getClass().getResourceAsStream("ror_org.json")), RorOrganization.class);
		final List<AtomicAction<? extends Oaf>> aas = GenerateRorActionSetJob.convertRorOrg(r);

		Assertions.assertEquals(3, aas.size());
		assertEquals(Organization.class, aas.get(0).getClazz());
		assertEquals(Relation.class, aas.get(1).getClazz());
		assertEquals(Relation.class, aas.get(2).getClazz());

		final Organization o = (Organization) aas.get(0).getPayload();
		final Relation r1 = (Relation) aas.get(1).getPayload();
		final Relation r2 = (Relation) aas.get(2).getPayload();

		assertEquals(o.getId(), r1.getSource());
		assertEquals(r1.getSource(), r2.getTarget());
		assertEquals(r2.getSource(), r1.getTarget());
		assertEquals(ModelConstants.IS_PARENT_OF, r1.getRelClass());
		assertEquals(ModelConstants.IS_CHILD_OF, r2.getRelClass());

		System.out.println(mapper.writeValueAsString(o));
		System.out.println(mapper.writeValueAsString(r1));
		System.out.println(mapper.writeValueAsString(r2));

	}

	@Test
	@Disabled
	void testConvertAllRorOrg() throws Exception {
		final RorOrganization[] arr = mapper
			.readValue(IOUtils.toString(new FileInputStream(local_file_path)), RorOrganization[].class);

		for (final RorOrganization r : arr) {
			final List<AtomicAction<? extends Oaf>> aas = GenerateRorActionSetJob.convertRorOrg(r);
			Assertions.assertFalse(aas.isEmpty());
			Assertions.assertNotNull(aas.get(0));
			final Organization o = (Organization) aas.get(0).getPayload();
			Assertions.assertTrue(StringUtils.isNotBlank(o.getId()));
		}
	}

}
