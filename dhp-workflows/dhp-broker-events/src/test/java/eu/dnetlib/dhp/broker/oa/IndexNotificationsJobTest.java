
package eu.dnetlib.dhp.broker.oa;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.model.MappedFields;
import eu.dnetlib.dhp.broker.model.Subscription;
import eu.dnetlib.dhp.broker.oa.util.NotificationGroup;

class IndexNotificationsJobTest {

	private List<Subscription> subscriptions;

	@BeforeEach
	void setUp() throws Exception {
		final Subscription s = new Subscription();
		s.setTopic("ENRICH/MISSING/PID");
		s.setConditions("[{\"field\":\"targetDatasourceName\",\"fieldType\":\"STRING\",\"operator\":\"EXACT\",\"listParams\":[{\"value\":\"reposiTUm\"}]},{\"field\":\"trust\",\"fieldType\":\"FLOAT\",\"operator\":\"RANGE\",\"listParams\":[{\"value\":\"0\",\"otherValue\":\"1\"}]}]");
		subscriptions = Arrays.asList(s);
		IndexNotificationsJob.initConditionsForSubscriptions(subscriptions);
	}

	@Test
	void testGenerateNotifications_invalid_topic() {
		final Event event = new Event();
		event.setTopic("ENRICH/MISSING/PROJECT");

		for (int i = 0; i < 10; i++) {
			final long start = System.currentTimeMillis();
			final NotificationGroup res = IndexNotificationsJob.generateNotifications(event, subscriptions, 0);
			final long end = System.currentTimeMillis();

			System.out.println("no topic - execution time (ms): " + (end - start));

			assertEquals(0, res.getData().size());
		}
	}

	@Test
	void testGenerateNotifications_topic_match() {
		final Event event = new Event();
		event.setTopic("ENRICH/MISSING/PID");
		event.setMap(new MappedFields());
		event.getMap().setTargetDatasourceName("reposiTUm");
		event.getMap().setTrust(0.8f);

		for (int i = 0; i < 10; i++) {
			final long start = System.currentTimeMillis();
			final NotificationGroup res = IndexNotificationsJob.generateNotifications(event, subscriptions, 0);
			final long end = System.currentTimeMillis();

			System.out.println("topic match - execution time (ms): " + (end - start));

			assertEquals(1, res.getData().size());
		}
	}

	@Test
	void testGenerateNotifications_topic_no_match() {
		final Event event = new Event();
		event.setTopic("ENRICH/MISSING/PID");
		event.setMap(new MappedFields());
		event.getMap().setTargetDatasourceName("Puma");
		event.getMap().setTrust(0.8f);

		for (int i = 0; i < 10; i++) {
			final long start = System.currentTimeMillis();
			final NotificationGroup res = IndexNotificationsJob.generateNotifications(event, subscriptions, 0);
			final long end = System.currentTimeMillis();

			System.out.println("topic no match - execution time (ms): " + (end - start));

			assertEquals(0, res.getData().size());
		}
	}

}
