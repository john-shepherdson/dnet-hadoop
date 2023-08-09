
package eu.dnetlib.dhp.schema.oaf.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class PmcCleaningRuleTest {

	@Test
	void testCleaning() {
		assertEquals("PMC1234", PmcCleaningRule.clean("PMC1234"));
		assertEquals("PMC1234", PmcCleaningRule.clean(" PMC1234"));
		assertEquals("PMC12345678", PmcCleaningRule.clean("PMC12345678"));
		assertEquals("PMC12345678", PmcCleaningRule.clean("PMC123456789"));
		assertEquals("PMC12345678", PmcCleaningRule.clean("PMC 12345678"));
	}

}
