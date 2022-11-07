
package eu.dnetlib.dhp.blacklist;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.common.RelationInverse;

public class BlacklistRelationTest {

	@Test
	public void testRelationInverseLookup() {

		final List<String> rels = Arrays
			.asList(
				"resultResult_relationship_IsRelatedTo",
				"resultOrganization_affiliation_isAuthorInstitutionOf",
				"resultOrganization_affiliation_hasAuthorInstitution",
				"datasourceOrganization_provision_isProvidedBy",
				"projectOrganization_participation_hasParticipant",
				"resultProject_outcome_produces",
				"resultProject_outcome_isProducedBy");

		rels.forEach(r -> {
			RelationInverse inverse = ModelSupport.relationInverseMap.get(r);
			Assertions.assertNotNull(inverse);
			Assertions.assertNotNull(inverse.getRelType());
			Assertions.assertNotNull(inverse.getSubReltype());
			Assertions.assertNotNull(inverse.getRelClass());
		});

	}

}
