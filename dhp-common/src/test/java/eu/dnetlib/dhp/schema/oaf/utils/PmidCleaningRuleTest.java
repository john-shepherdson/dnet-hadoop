
package eu.dnetlib.dhp.schema.oaf.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class PmidCleaningRuleTest {

	@Test
	void testCleaning() {
		// leading zeros are removed
		assertEquals("1234", PmidCleaningRule.clean("01234"));
		// tolerant to spaces in the middle
		assertEquals("1234567", PmidCleaningRule.clean("0123 4567"));
		// stop parsing at first not numerical char
		assertEquals("123", PmidCleaningRule.clean("0123x4567"));
		// invalid id leading to empty result
		assertEquals("", PmidCleaningRule.clean("abc"));
		// valid id with zeroes in the number
		assertEquals("20794075", PmidCleaningRule.clean("20794075"));
	}

}
