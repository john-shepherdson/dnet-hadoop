
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Software;

public class EnrichMissingSoftware
	extends UpdateMatcher<Pair<Result, List<Software>>, eu.dnetlib.broker.objects.Software> {

	public EnrichMissingSoftware() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<eu.dnetlib.broker.objects.Software>> findUpdates(
		final Pair<Result, List<Software>> source,
		final Pair<Result, List<Software>> target) {
		// TODO
		return Arrays.asList();
	}

	@Override
	public UpdateInfo<eu.dnetlib.broker.objects.Software> generateUpdateInfo(
		final eu.dnetlib.broker.objects.Software highlightValue,
		final Pair<Result, List<Software>> source,
		final Pair<Result, List<Software>> target) {
		return new UpdateInfo<>(
			Topic.ENRICH_MISSING_SOFTWARE,
			highlightValue, source.getLeft(), target.getLeft(),
			(p, s) -> p.getSoftwares().add(s),
			s -> s.getName());
	}

}
