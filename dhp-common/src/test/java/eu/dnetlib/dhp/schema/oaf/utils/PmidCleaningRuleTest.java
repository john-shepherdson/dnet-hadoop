
package eu.dnetlib.dhp.schema.oaf.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class PmidCleaningRuleTest {

	@Test
	void testCleaning() {
		assertEquals("1234", PmidCleaningRule.clean("01234"));
		assertEquals("1234567", PmidCleaningRule.clean("0123 4567"));
		assertEquals("123", PmidCleaningRule.clean("0123x4567"));
		assertEquals("", PmidCleaningRule.clean("abc"));
	}

}
