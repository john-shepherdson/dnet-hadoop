
package eu.dnetlib.dhp.broker.oa.matchers.relatedPublications;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedPublication;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.pace.config.DedupConfig;

public abstract class AbstractEnrichMissingPublication
	extends UpdateMatcher<eu.dnetlib.broker.objects.Publication> {

	private final Topic topic;

	public AbstractEnrichMissingPublication(final Topic topic) {
		super(true);
		this.topic = topic;
	}

	protected abstract boolean filterByType(String relType);

	@Override
	protected final List<UpdateInfo<eu.dnetlib.broker.objects.Publication>> findUpdates(
		final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {

		final Set<String> existingPublications = target
			.getPublications()
			.stream()
			.filter(rel -> filterByType(rel.getRelType()))
			.map(RelatedPublication::getRelPublication)
			.map(Publication::getId)
			.collect(Collectors.toSet());

		return source
			.getPublications()
			.stream()
			.filter(rel -> filterByType(rel.getRelType()))
			.map(RelatedPublication::getRelPublication)
			.filter(d -> !existingPublications.contains(d.getId()))
			.map(ConversionUtils::oafResultToBrokerPublication)
			.map(i -> generateUpdateInfo(i, source, target, dedupConfig))
			.collect(Collectors.toList());

	}

	protected final UpdateInfo<eu.dnetlib.broker.objects.Publication> generateUpdateInfo(
		final eu.dnetlib.broker.objects.Publication highlightValue,
		final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {
		return new UpdateInfo<>(
			getTopic(),
			highlightValue, source, target,
			(p, rel) -> p.getPublications().add(rel),
			rel -> rel.getInstances().get(0).getUrl(), dedupConfig);
	}

	public Topic getTopic() {
		return topic;
	}
}
