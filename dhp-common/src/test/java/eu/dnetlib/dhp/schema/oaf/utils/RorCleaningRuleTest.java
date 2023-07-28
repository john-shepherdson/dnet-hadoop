
package eu.dnetlib.dhp.schema.oaf.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class RorCleaningRuleTest {

	@Test
	void testCleaning() {
		assertEquals("https://ror.org/05rpz9w55", RorCleaningRule.clean("https://ror.org/05rpz9w55"));
		assertEquals("https://ror.org/05rpz9w55", RorCleaningRule.clean("05rpz9w55"));
		assertEquals("", RorCleaningRule.clean("05rpz9w_55"));
	}

}
