
package eu.dnetlib.dhp.broker.oa.matchers;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.Pid;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.schema.oaf.Result;

public class EnrichMorePid extends UpdateMatcher<Pid> {

	public EnrichMorePid() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<Pid>> findUpdates(final Result source, final Result target) {
		final Set<String> existingPids = target
			.getPid()
			.stream()
			.map(pid -> pid.getQualifier().getClassid() + "::" + pid.getValue())
			.collect(Collectors.toSet());

		return source
			.getPid()
			.stream()
			.filter(pid -> !existingPids.contains(pid.getQualifier().getClassid() + "::" + pid.getValue()))
			.map(ConversionUtils::oafPidToBrokerPid)
			.map(i -> generateUpdateInfo(i, source, target))
			.collect(Collectors.toList());
	}

	@Override
	public UpdateInfo<Pid> generateUpdateInfo(final Pid highlightValue, final Result source, final Result target) {
		return new UpdateInfo<>(
			Topic.ENRICH_MORE_PID,
			highlightValue, source, target,
			(p, pid) -> p.getPids().add(pid),
			pid -> pid.getType() + "::" + pid.getValue());
	}

}
