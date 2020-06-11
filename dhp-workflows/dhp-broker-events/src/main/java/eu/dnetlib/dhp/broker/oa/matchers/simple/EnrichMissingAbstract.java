
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;
import eu.dnetlib.pace.config.DedupConfig;

public class EnrichMissingAbstract extends UpdateMatcher<String> {

	public EnrichMissingAbstract() {
		super(false);
	}

	@Override
	protected List<UpdateInfo<String>> findUpdates(final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {
		if (isMissing(target.getResult().getDescription()) && !isMissing(source.getResult().getDescription())) {
			return Arrays
				.asList(
					generateUpdateInfo(
						source.getResult().getDescription().get(0).getValue(), source, target, dedupConfig));
		}
		return new ArrayList<>();
	}

	public UpdateInfo<String> generateUpdateInfo(final String highlightValue,
		final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {
		return new UpdateInfo<>(
			Topic.ENRICH_MISSING_ABSTRACT,
			highlightValue, source, target,
			(p, s) -> p.getAbstracts().add(s),
			s -> s, dedupConfig);
	}

}
