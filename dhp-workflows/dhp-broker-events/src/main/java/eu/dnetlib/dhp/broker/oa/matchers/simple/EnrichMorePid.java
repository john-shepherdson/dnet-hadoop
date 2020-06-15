
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.Pid;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;

public class EnrichMorePid extends UpdateMatcher<Pid> {

	public EnrichMorePid() {
		super(true,
			pid -> Topic.ENRICH_MORE_PID,
			(p, pid) -> p.getPids().add(pid),
			pid -> pid.getType() + "::" + pid.getValue());
	}

	@Override
	protected List<Pid> findDifferences(final ResultWithRelations source,
		final ResultWithRelations target) {
		final Set<String> existingPids = target
			.getResult()
			.getPid()
			.stream()
			.map(pid -> pid.getQualifier().getClassid() + "::" + pid.getValue())
			.collect(Collectors.toSet());

		return source
			.getResult()
			.getPid()
			.stream()
			.filter(pid -> !existingPids.contains(pid.getQualifier().getClassid() + "::" + pid.getValue()))
			.map(ConversionUtils::oafPidToBrokerPid)
			.collect(Collectors.toList());
	}

}
