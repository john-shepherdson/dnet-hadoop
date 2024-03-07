
package eu.dnetlib.dhp.schema.oaf.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class GridCleaningRuleTest {

	@Test
	void testCleaning() {
		assertEquals("grid.493784.5", GridCleaningRule.clean("grid.493784.5"));
		assertEquals("grid.493784.5x", GridCleaningRule.clean("grid.493784.5x"));
		assertEquals("grid.493784.5x", GridCleaningRule.clean("493784.5x"));
		assertEquals("", GridCleaningRule.clean("493x784.5x"));
	}

}
