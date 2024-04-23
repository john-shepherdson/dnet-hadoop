
package eu.dnetlib.pace.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import eu.dnetlib.pace.model.Person;
import jdk.nashorn.internal.ir.annotations.Ignore;

public class UtilTest {

	static Map<String, String> params;

	@BeforeAll
	public static void setUp() {
		params = new HashMap<>();
	}

	@Test
	@Disabled
	public void paceResolverTest() {
		PaceResolver paceResolver = new PaceResolver();
		paceResolver.getComparator("keywordMatch", params);
	}

	@Test
	public void personTest() {
		Person p = new Person("j. f. kennedy", false);

		assertEquals("kennedy", p.getSurnameString());
		assertEquals("j f", p.getNameString());

		p = new Person("Guan-Hua Du", false);

		System.out.println("surname = " + p.getSurnameString());
		System.out.println("name = " + p.getNameString());
	}

}
