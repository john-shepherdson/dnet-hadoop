
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.Pid;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;

public class EnrichMissingPid extends UpdateMatcher<Pid> {

	public EnrichMissingPid() {
		super(true,
			pid -> Topic.ENRICH_MISSING_PID,
			(p, pid) -> p.getPids().add(pid),
			pid -> pid.getType() + "::" + pid.getValue());
	}

	@Override
	protected List<Pid> findDifferences(final ResultWithRelations source,
		final ResultWithRelations target) {
		final long count = target.getResult().getPid().size();

		if (count > 0) {
			return Arrays.asList();
		}

		return source
			.getResult()
			.getPid()
			.stream()
			.map(ConversionUtils::oafPidToBrokerPid)
			.collect(Collectors.toList());
	}

}
