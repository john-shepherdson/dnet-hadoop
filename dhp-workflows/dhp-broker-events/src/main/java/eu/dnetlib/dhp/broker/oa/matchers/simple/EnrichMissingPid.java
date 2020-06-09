
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.Pid;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.pace.config.DedupConfig;

public class EnrichMissingPid extends UpdateMatcher<Result, Pid> {

	public EnrichMissingPid() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<Pid>> findUpdates(final Result source, final Result target, final DedupConfig dedupConfig) {
		final long count = target.getPid().size();

		if (count > 0) { return Arrays.asList(); }

		return source
			.getPid()
			.stream()
			.map(ConversionUtils::oafPidToBrokerPid)
			.map(i -> generateUpdateInfo(i, source, target, dedupConfig))
			.collect(Collectors.toList());
	}

	public UpdateInfo<Pid> generateUpdateInfo(final Pid highlightValue, final Result source, final Result target, final DedupConfig dedupConfig) {
		return new UpdateInfo<>(
			Topic.ENRICH_MISSING_PID,
			highlightValue, source, target,
			(p, pid) -> p.getPids().add(pid),
			pid -> pid.getType() + "::" + pid.getValue(), dedupConfig);
	}

}
