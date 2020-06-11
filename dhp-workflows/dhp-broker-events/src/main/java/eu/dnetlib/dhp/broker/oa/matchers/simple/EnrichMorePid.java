
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.Pid;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;
import eu.dnetlib.pace.config.DedupConfig;

public class EnrichMorePid extends UpdateMatcher<Pid> {

	public EnrichMorePid() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<Pid>> findUpdates(final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {
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
			.map(i -> generateUpdateInfo(i, source, target, dedupConfig))
			.collect(Collectors.toList());
	}

	public UpdateInfo<Pid> generateUpdateInfo(final Pid highlightValue,
		final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {
		return new UpdateInfo<>(
			Topic.ENRICH_MORE_PID,
			highlightValue, source, target,
			(p, pid) -> p.getPids().add(pid),
			pid -> pid.getType() + "::" + pid.getValue(), dedupConfig);
	}

}
