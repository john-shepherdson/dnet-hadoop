
package eu.dnetlib.dhp.common;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class PacePersonTest {

	@Test
	void pacePersonTest1() {

		PacePerson p = new PacePerson("Artini, Michele", false);
		assertEquals("Artini", p.getSurnameString());
		assertEquals("Michele", p.getNameString());
		assertEquals("Artini, Michele", p.getNormalisedFullname());
	}

	@Test
	void pacePersonTest2() {
		PacePerson p = new PacePerson("Michele G. Artini", false);
		assertEquals("Artini, Michele G.", p.getNormalisedFullname());
		assertEquals("Michele G", p.getNameString());
		assertEquals("Artini", p.getSurnameString());
	}

}
