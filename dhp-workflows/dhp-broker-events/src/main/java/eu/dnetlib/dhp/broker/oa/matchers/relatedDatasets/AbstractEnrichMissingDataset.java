
package eu.dnetlib.dhp.broker.oa.matchers.relatedDatasets;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedDataset;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.pace.config.DedupConfig;

public abstract class AbstractEnrichMissingDataset
	extends UpdateMatcher<eu.dnetlib.broker.objects.Dataset> {

	private final Topic topic;

	public AbstractEnrichMissingDataset(final Topic topic) {
		super(true);
		this.topic = topic;
	}

	protected abstract boolean filterByType(String relType);

	@Override
	protected final List<UpdateInfo<eu.dnetlib.broker.objects.Dataset>> findUpdates(
		final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {

		final Set<String> existingDatasets = target
			.getDatasets()
			.stream()
			.filter(rel -> filterByType(rel.getRelType()))
			.map(RelatedDataset::getRelDataset)
			.map(Dataset::getId)
			.collect(Collectors.toSet());

		return source
			.getDatasets()
			.stream()
			.filter(rel -> filterByType(rel.getRelType()))
			.map(RelatedDataset::getRelDataset)
			.filter(d -> !existingDatasets.contains(d.getId()))
			.map(ConversionUtils::oafDatasetToBrokerDataset)
			.map(i -> generateUpdateInfo(i, source, target, dedupConfig))
			.collect(Collectors.toList());

	}

	protected final UpdateInfo<eu.dnetlib.broker.objects.Dataset> generateUpdateInfo(
		final eu.dnetlib.broker.objects.Dataset highlightValue,
		final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {
		return new UpdateInfo<>(
			getTopic(),
			highlightValue, source, target,
			(p, rel) -> p.getDatasets().add(rel),
			rel -> rel.getInstances().get(0).getUrl(),
			dedupConfig);
	}

	public Topic getTopic() {
		return topic;
	}

}
