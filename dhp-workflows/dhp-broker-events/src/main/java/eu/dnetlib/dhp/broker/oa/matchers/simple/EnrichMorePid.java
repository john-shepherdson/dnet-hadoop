
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.OpenaireBrokerResult;
import eu.dnetlib.broker.objects.TypedValue;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;

public class EnrichMorePid extends UpdateMatcher<TypedValue> {

	public EnrichMorePid() {
		super(true,
			pid -> Topic.ENRICH_MORE_PID,
			(p, pid) -> p.getPids().add(pid),
			pid -> pidAsString(pid));
	}

	@Override
	protected List<TypedValue> findDifferences(final OpenaireBrokerResult source,
		final OpenaireBrokerResult target) {
		final Set<String> existingPids = target
			.getPids()
			.stream()
			.map(pid -> pidAsString(pid))
			.collect(Collectors.toSet());

		return source
			.getPids()
			.stream()
			.filter(pid -> !existingPids.contains(pidAsString(pid)))
			.collect(Collectors.toList());
	}

	private static String pidAsString(final TypedValue pid) {
		return pid.getType() + "::" + pid.getValue();
	}
}
