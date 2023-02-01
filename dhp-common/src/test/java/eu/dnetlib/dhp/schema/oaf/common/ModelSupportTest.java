
package eu.dnetlib.dhp.schema.oaf.common;

import eu.dnetlib.dhp.schema.oaf.Entity;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class ModelSupportTest {

	@Nested
	class IsSubClass {

		@Test
		void shouldReturnFalseWhenSubClassDoesNotExtendSuperClass() {
			// when
			Boolean result = ModelSupport.isSubClass(Relation.class, Entity.class);

			// then
			assertFalse(result);
		}

		@Test
		void shouldReturnTrueWhenSubClassExtendsSuperClass() {
			// when
			Boolean result = ModelSupport.isSubClass(Result.class, Entity.class);

			// then
			assertTrue(result);
		}
	}


	@Nested
	class InverseRelation {

		@Test
		void findRelations() throws IOException {
			assertNotNull(ModelSupport.findRelation("isMetadataFor"));
			assertNotNull(ModelSupport.findRelation("ismetadatafor"));
			assertNotNull(ModelSupport.findRelation("ISMETADATAFOR"));
			assertNotNull(ModelSupport.findRelation("isRelatedTo"));


		}
	}
}
