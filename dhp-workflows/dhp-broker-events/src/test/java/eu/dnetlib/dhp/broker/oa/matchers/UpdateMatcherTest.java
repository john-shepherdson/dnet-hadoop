
package eu.dnetlib.dhp.broker.oa.matchers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.broker.objects.OaBrokerRelatedDatasource;
import eu.dnetlib.dhp.broker.oa.matchers.simple.EnrichMissingPublicationDate;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;

@ExtendWith(MockitoExtension.class)
public class UpdateMatcherTest {

	UpdateMatcher<String> matcher = new EnrichMissingPublicationDate();

	@Mock
	private OaBrokerRelatedDatasource targetDs;

	@BeforeEach
	void setUp() throws Exception {
	}

	@Test
	void testSearchUpdatesForRecord_1() {
		final OaBrokerMainEntity res = new OaBrokerMainEntity();
		final OaBrokerMainEntity p1 = new OaBrokerMainEntity();
		final OaBrokerMainEntity p2 = new OaBrokerMainEntity();
		final OaBrokerMainEntity p3 = new OaBrokerMainEntity();
		final OaBrokerMainEntity p4 = new OaBrokerMainEntity();

		final Collection<UpdateInfo<String>> list = matcher
			.searchUpdatesForRecord(res, targetDs, Arrays.asList(p1, p2, p3, p4), null);

		assertTrue(list.isEmpty());
	}

	@Test
	void testSearchUpdatesForRecord_2() {
		final OaBrokerMainEntity res = new OaBrokerMainEntity();
		final OaBrokerMainEntity p1 = new OaBrokerMainEntity();
		final OaBrokerMainEntity p2 = new OaBrokerMainEntity();
		final OaBrokerMainEntity p3 = new OaBrokerMainEntity();
		final OaBrokerMainEntity p4 = new OaBrokerMainEntity();

		res.setPublicationdate("2018");

		final Collection<UpdateInfo<String>> list = matcher
			.searchUpdatesForRecord(res, targetDs, Arrays.asList(p1, p2, p3, p4), null);

		assertTrue(list.isEmpty());
	}

	@Test
	void testSearchUpdatesForRecord_3() {
		final OaBrokerMainEntity res = new OaBrokerMainEntity();
		final OaBrokerMainEntity p1 = new OaBrokerMainEntity();
		final OaBrokerMainEntity p2 = new OaBrokerMainEntity();
		final OaBrokerMainEntity p3 = new OaBrokerMainEntity();
		final OaBrokerMainEntity p4 = new OaBrokerMainEntity();

		p2.setPublicationdate("2018");

		final Collection<UpdateInfo<String>> list = matcher
			.searchUpdatesForRecord(res, targetDs, Arrays.asList(p1, p2, p3, p4), null);

		assertEquals(1, list.size());
	}

	@Test
	void testSearchUpdatesForRecord_4() {
		final OaBrokerMainEntity res = new OaBrokerMainEntity();
		final OaBrokerMainEntity p1 = new OaBrokerMainEntity();
		final OaBrokerMainEntity p2 = new OaBrokerMainEntity();
		final OaBrokerMainEntity p3 = new OaBrokerMainEntity();
		final OaBrokerMainEntity p4 = new OaBrokerMainEntity();

		res.setPublicationdate("2018");
		p2.setPublicationdate("2018");

		final Collection<UpdateInfo<String>> list = matcher
			.searchUpdatesForRecord(res, targetDs, Arrays.asList(p1, p2, p3, p4), null);

		assertTrue(list.isEmpty());
	}

	@Test
	void testSearchUpdatesForRecord_5() {
		final OaBrokerMainEntity res = new OaBrokerMainEntity();
		final OaBrokerMainEntity p1 = new OaBrokerMainEntity();
		final OaBrokerMainEntity p2 = new OaBrokerMainEntity();
		final OaBrokerMainEntity p3 = new OaBrokerMainEntity();
		final OaBrokerMainEntity p4 = new OaBrokerMainEntity();
		res.setPublicationdate("2018");
		p1.setPublicationdate("2018");
		p2.setPublicationdate("2018");
		p3.setPublicationdate("2018");
		p4.setPublicationdate("2018");

		final Collection<UpdateInfo<String>> list = matcher
			.searchUpdatesForRecord(res, targetDs, Arrays.asList(p1, p2, p3, p4), null);

		assertTrue(list.isEmpty());
	}

	@Test
	void testSearchUpdatesForRecord_6() {
		final OaBrokerMainEntity res = new OaBrokerMainEntity();
		final OaBrokerMainEntity p1 = new OaBrokerMainEntity();
		final OaBrokerMainEntity p2 = new OaBrokerMainEntity();
		final OaBrokerMainEntity p3 = new OaBrokerMainEntity();
		final OaBrokerMainEntity p4 = new OaBrokerMainEntity();

		p1.setPublicationdate("2018");
		p2.setPublicationdate("2018");
		p3.setPublicationdate("2018");
		p4.setPublicationdate("2018");

		final Collection<UpdateInfo<String>> list = matcher
			.searchUpdatesForRecord(res, targetDs, Arrays.asList(p1, p2, p3, p4), null);

		assertEquals(1, list.size());
	}

}
