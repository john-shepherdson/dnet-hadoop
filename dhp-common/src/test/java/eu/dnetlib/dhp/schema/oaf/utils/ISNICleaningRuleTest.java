
package eu.dnetlib.dhp.schema.oaf.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ISNICleaningRuleTest {

	@Test
	void testCleaning() {
		assertEquals("0000000463436020", ISNICleaningRule.clean("0000 0004 6343 6020"));
		assertEquals("0000000463436020", ISNICleaningRule.clean("0000000463436020"));
		assertEquals("", ISNICleaningRule.clean("Q30256598"));
		assertEquals("0000000493403529", ISNICleaningRule.clean("ISNI:0000000493403529"));
		assertEquals("000000008614884X", ISNICleaningRule.clean("0000 0000 8614 884X"));
	}

}
