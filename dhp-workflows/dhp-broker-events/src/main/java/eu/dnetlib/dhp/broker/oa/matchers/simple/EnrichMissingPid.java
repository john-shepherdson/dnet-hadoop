
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.broker.objects.OaBrokerTypedValue;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;

public class EnrichMissingPid extends UpdateMatcher<OaBrokerTypedValue> {

	public EnrichMissingPid() {
		super(true,
			pid -> Topic.ENRICH_MISSING_PID,
			(p, pid) -> p.getPids().add(pid),
			pid -> pid.getType() + "::" + pid.getValue());
	}

	@Override
	protected List<OaBrokerTypedValue> findDifferences(final OaBrokerMainEntity source,
		final OaBrokerMainEntity target) {
		final long count = target.getPids().size();

		if (count > 0) {
			return Arrays.asList();
		}

		return source
			.getPids()
			.stream()
			.collect(Collectors.toList());
	}

}
