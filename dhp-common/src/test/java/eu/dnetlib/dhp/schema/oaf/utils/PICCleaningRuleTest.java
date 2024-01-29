
package eu.dnetlib.dhp.schema.oaf.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class PICCleaningRuleTest {

	@Test
	void testCleaning() {
		assertEquals("887624982", PICCleaningRule.clean("887624982"));
		assertEquals("", PICCleaningRule.clean("887 624982"));
		assertEquals("887624982", PICCleaningRule.clean(" 887624982 "));
		assertEquals("887624982", PICCleaningRule.clean(" 887624982x "));
		assertEquals("887624982", PICCleaningRule.clean(" 88762498200 "));
	}

}
