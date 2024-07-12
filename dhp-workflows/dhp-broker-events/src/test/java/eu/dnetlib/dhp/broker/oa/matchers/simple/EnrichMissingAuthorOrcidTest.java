package eu.dnetlib.dhp.broker.oa.matchers.simple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.dnetlib.broker.objects.OaBrokerAuthor;
import eu.dnetlib.broker.objects.OaBrokerMainEntity;

class EnrichMissingAuthorOrcidTest {

	final EnrichMissingAuthorOrcid matcher = new EnrichMissingAuthorOrcid();

	@BeforeEach
	void setUp() throws Exception {}

	@Test
	void testFindDifferences_1() {
		final OaBrokerMainEntity source = new OaBrokerMainEntity();
		final OaBrokerMainEntity target = new OaBrokerMainEntity();
		final List<OaBrokerAuthor> list = this.matcher.findDifferences(source, target);
		assertTrue(list.isEmpty());
	}

	@Test
	void testFindDifferences_2() {
		final OaBrokerMainEntity source = new OaBrokerMainEntity();
		final OaBrokerMainEntity target = new OaBrokerMainEntity();

		source.getCreators().add(new OaBrokerAuthor("Claudio Atzori", "0000-0001-9613-6639"));
		target.getCreators().add(new OaBrokerAuthor("Claudio Atzori", null));

		final List<OaBrokerAuthor> list = this.matcher.findDifferences(source, target);
		assertEquals(1, list.size());
	}

	@Test
	void testFindDifferences_3() {
		final OaBrokerMainEntity source = new OaBrokerMainEntity();
		final OaBrokerMainEntity target = new OaBrokerMainEntity();

		source.getCreators().add(new OaBrokerAuthor("Claudio Atzori", null));
		target.getCreators().add(new OaBrokerAuthor("Claudio Atzori", "0000-0001-9613-6639"));

		final List<OaBrokerAuthor> list = this.matcher.findDifferences(source, target);
		assertTrue(list.isEmpty());
	}

	@Test
	void testFindDifferences_4() {
		final OaBrokerMainEntity source = new OaBrokerMainEntity();
		final OaBrokerMainEntity target = new OaBrokerMainEntity();
		source.getCreators().add(new OaBrokerAuthor("Claudio Atzori", "0000-0001-9613-6639"));
		target.getCreators().add(new OaBrokerAuthor("Claudio Atzori", "0000-0001-9613-6639"));

		final List<OaBrokerAuthor> list = this.matcher.findDifferences(source, target);
		assertTrue(list.isEmpty());
	}

}
