
package eu.dnetlib.dhp.broker.oa;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import eu.dnetlib.dhp.broker.model.ConditionParams;
import eu.dnetlib.dhp.broker.model.Event;
import eu.dnetlib.dhp.broker.model.MappedFields;
import eu.dnetlib.dhp.broker.model.Subscription;
import eu.dnetlib.dhp.broker.oa.util.NotificationGroup;

class GenerateNotificationsJobTest {

	private List<Subscription> subscriptions;

	private Map<String, Map<String, List<ConditionParams>>> conditionsMap;

	private static final int N_TIMES = 1_000_000;

	@BeforeEach
	void setUp() throws Exception {
		final Subscription s = new Subscription();
		s.setTopic("ENRICH/MISSING/PID");
		s
			.setConditions(
				"[{\"field\":\"targetDatasourceName\",\"fieldType\":\"STRING\",\"operator\":\"EXACT\",\"listParams\":[{\"value\":\"reposiTUm\"}]},{\"field\":\"trust\",\"fieldType\":\"FLOAT\",\"operator\":\"RANGE\",\"listParams\":[{\"value\":\"0\",\"otherValue\":\"1\"}]}]");
		subscriptions = Arrays.asList(s);
		conditionsMap = GenerateNotificationsJob.prepareConditionsMap(subscriptions);
	}

	@Test
	void testGenerateNotifications_invalid_topic() {
		final Event event = new Event();
		event.setTopic("ENRICH/MISSING/PROJECT");

		final NotificationGroup res = GenerateNotificationsJob
			.generateNotifications(event, subscriptions, conditionsMap, 0);
		assertEquals(0, res.getData().size());
	}

	@Test
	void testGenerateNotifications_topic_match() {
		final Event event = new Event();
		event.setTopic("ENRICH/MISSING/PID");
		event.setMap(new MappedFields());
		event.getMap().setTargetDatasourceName("reposiTUm");
		event.getMap().setTrust(0.8f);

		final NotificationGroup res = GenerateNotificationsJob
			.generateNotifications(event, subscriptions, conditionsMap, 0);
		assertEquals(1, res.getData().size());
	}

	@Test
	void testGenerateNotifications_topic_no_match() {
		final Event event = new Event();
		event.setTopic("ENRICH/MISSING/PID");
		event.setMap(new MappedFields());
		event.getMap().setTargetDatasourceName("Puma");
		event.getMap().setTrust(0.8f);

		final NotificationGroup res = GenerateNotificationsJob
			.generateNotifications(event, subscriptions, conditionsMap, 0);
		assertEquals(0, res.getData().size());
	}

	@Test
	void testGenerateNotifications_invalid_topic_repeated() {
		final Event event = new Event();
		event.setTopic("ENRICH/MISSING/PROJECT");

		// warm up
		GenerateNotificationsJob.generateNotifications(event, subscriptions, conditionsMap, 0);

		final long start = System.currentTimeMillis();
		for (int i = 0; i < N_TIMES; i++) {
			GenerateNotificationsJob.generateNotifications(event, subscriptions, conditionsMap, 0);
		}
		final long end = System.currentTimeMillis();
		System.out
			.println(String.format("no topic - repeated %s times - execution time: %s ms ", N_TIMES, end - start));

	}

	@Test
	void testGenerateNotifications_topic_match_repeated() {
		final Event event = new Event();
		event.setTopic("ENRICH/MISSING/PID");
		event.setMap(new MappedFields());
		event.getMap().setTargetDatasourceName("reposiTUm");
		event.getMap().setTrust(0.8f);

		// warm up
		GenerateNotificationsJob.generateNotifications(event, subscriptions, conditionsMap, 0);

		final long start = System.currentTimeMillis();
		for (int i = 0; i < N_TIMES; i++) {
			GenerateNotificationsJob.generateNotifications(event, subscriptions, conditionsMap, 0);
		}
		final long end = System.currentTimeMillis();
		System.out
			.println(String.format("topic match - repeated %s times - execution time: %s ms ", N_TIMES, end - start));
	}

	@Test
	void testGenerateNotifications_topic_no_match_repeated() {
		final Event event = new Event();
		event.setTopic("ENRICH/MISSING/PID");
		event.setMap(new MappedFields());
		event.getMap().setTargetDatasourceName("Puma");
		event.getMap().setTrust(0.8f);

		// warm up
		GenerateNotificationsJob.generateNotifications(event, subscriptions, conditionsMap, 0);

		final long start = System.currentTimeMillis();
		for (int i = 0; i < N_TIMES; i++) {
			GenerateNotificationsJob.generateNotifications(event, subscriptions, conditionsMap, 0);
		}
		final long end = System.currentTimeMillis();
		System.out
			.println(
				String.format("topic no match - repeated %s times - execution time: %s ms ", N_TIMES, end - start));
	}

}
