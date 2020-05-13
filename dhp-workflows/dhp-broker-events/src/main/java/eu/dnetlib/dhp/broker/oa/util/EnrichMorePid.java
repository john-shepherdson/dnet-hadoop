
package eu.dnetlib.dhp.broker.oa.util;

import java.util.Arrays;
import java.util.List;

import eu.dnetlib.broker.objects.Pid;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.schema.oaf.Result;

public class EnrichMorePid extends UpdateMatcher<Pid> {

	public EnrichMorePid() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<Pid>> findUpdates(final Result source, final Result target) {
		// return Arrays.asList(new EnrichMissingAbstract("xxxxxxx", 0.9f));
		return Arrays.asList();
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
