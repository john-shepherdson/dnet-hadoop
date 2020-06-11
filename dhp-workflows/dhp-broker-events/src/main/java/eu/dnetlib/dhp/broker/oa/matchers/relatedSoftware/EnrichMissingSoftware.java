
package eu.dnetlib.dhp.broker.oa.matchers.relatedSoftware;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedSoftware;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;
import eu.dnetlib.pace.config.DedupConfig;

public class EnrichMissingSoftware
	extends UpdateMatcher<eu.dnetlib.broker.objects.Software> {

	public EnrichMissingSoftware() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<eu.dnetlib.broker.objects.Software>> findUpdates(
		final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {

		if (source.getSoftwares().isEmpty()) {
			return Arrays.asList();
		} else {
			return target
				.getSoftwares()
				.stream()
				.map(RelatedSoftware::getRelSoftware)
				.map(ConversionUtils::oafSoftwareToBrokerSoftware)
				.map(p -> generateUpdateInfo(p, source, target, dedupConfig))
				.collect(Collectors.toList());
		}
	}

	public UpdateInfo<eu.dnetlib.broker.objects.Software> generateUpdateInfo(
		final eu.dnetlib.broker.objects.Software highlightValue,
		final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {
		return new UpdateInfo<>(
			Topic.ENRICH_MISSING_SOFTWARE,
			highlightValue, source, target,
			(p, s) -> p.getSoftwares().add(s),
			s -> s.getName(), dedupConfig);
	}

}
