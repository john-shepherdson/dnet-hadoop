
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;

public class EnrichMissingPublicationDateTest {

	final EnrichMissingPublicationDate matcher = new EnrichMissingPublicationDate();

	@BeforeEach
	void setUp() throws Exception {
	}

	@Test
	void testFindDifferences_1() {
		final OaBrokerMainEntity source = new OaBrokerMainEntity();
		final OaBrokerMainEntity target = new OaBrokerMainEntity();
		final List<String> list = matcher.findDifferences(source, target);
		assertTrue(list.isEmpty());
	}

	@Test
	void testFindDifferences_2() {
		final OaBrokerMainEntity source = new OaBrokerMainEntity();
		final OaBrokerMainEntity target = new OaBrokerMainEntity();
		source.setPublicationdate("2018");
		final List<String> list = matcher.findDifferences(source, target);
		assertEquals(1, list.size());
	}

	@Test
	void testFindDifferences_3() {
		final OaBrokerMainEntity source = new OaBrokerMainEntity();
		final OaBrokerMainEntity target = new OaBrokerMainEntity();
		target.setPublicationdate("2018");
		final List<String> list = matcher.findDifferences(source, target);
		assertTrue(list.isEmpty());
	}

	@Test
	void testFindDifferences_4() {
		final OaBrokerMainEntity source = new OaBrokerMainEntity();
		final OaBrokerMainEntity target = new OaBrokerMainEntity();
		source.setPublicationdate("2018");
		target.setPublicationdate("2018");
		final List<String> list = matcher.findDifferences(source, target);
		assertTrue(list.isEmpty());
	}

}
