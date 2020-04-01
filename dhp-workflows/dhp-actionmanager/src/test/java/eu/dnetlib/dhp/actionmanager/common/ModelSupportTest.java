package eu.dnetlib.dhp.actionmanager.common;

import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ModelSupportTest {

    @Nested
    class IsSubClass {

        @Test
        public void shouldReturnFalseWhenSubClassDoesNotExtendSuperClass() {
            // when
            Boolean result = ModelSupport.isSubClass(Relation.class, OafEntity.class);

            // then
            assertFalse(result);
        }

        @Test
        public void shouldReturnTrueWhenSubClassExtendsSuperClass() {
            // when
            Boolean result = ModelSupport.isSubClass(Result.class, OafEntity.class);

            // then
            assertTrue(result);
        }
    }
}