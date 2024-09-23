
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.broker.objects.OaBrokerTypedValue;

public class EnrichMoreSubjectTest {

	final EnrichMoreSubject matcher = new EnrichMoreSubject();

	@BeforeEach
	void setUp() throws Exception {
	}

	@Test
	void testFindDifferences_1() {
		final OaBrokerMainEntity source = new OaBrokerMainEntity();
		final OaBrokerMainEntity target = new OaBrokerMainEntity();
		final List<OaBrokerTypedValue> list = this.matcher.findDifferences(source, target);
		assertTrue(list.isEmpty());
	}

	@Test
	void testFindDifferences_2() {
		final OaBrokerMainEntity source = new OaBrokerMainEntity();
		final OaBrokerMainEntity target = new OaBrokerMainEntity();
		source.setSubjects(Arrays.asList(new OaBrokerTypedValue("arxiv", "subject_01")));
		final List<OaBrokerTypedValue> list = this.matcher.findDifferences(source, target);
		assertEquals(1, list.size());
	}

	@Test
	void testFindDifferences_3() {
		final OaBrokerMainEntity source = new OaBrokerMainEntity();
		final OaBrokerMainEntity target = new OaBrokerMainEntity();
		target.setSubjects(Arrays.asList(new OaBrokerTypedValue("arxiv", "subject_01")));
		final List<OaBrokerTypedValue> list = this.matcher.findDifferences(source, target);
		assertTrue(list.isEmpty());
	}

	@Test
	void testFindDifferences_4() {
		final OaBrokerMainEntity source = new OaBrokerMainEntity();
		final OaBrokerMainEntity target = new OaBrokerMainEntity();
		source.setSubjects(Arrays.asList(new OaBrokerTypedValue("arxiv", "subject_01")));
		target.setSubjects(Arrays.asList(new OaBrokerTypedValue("arxiv", "subject_01")));
		final List<OaBrokerTypedValue> list = this.matcher.findDifferences(source, target);
		assertTrue(list.isEmpty());
	}

}
