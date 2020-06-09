
package eu.dnetlib.dhp.broker.oa.matchers.relatedDatasets;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.pace.config.DedupConfig;

public abstract class AbstractEnrichMissingDataset
	extends UpdateMatcher<Pair<Result, List<Dataset>>, eu.dnetlib.broker.objects.Dataset> {

	private final Topic topic;

	public AbstractEnrichMissingDataset(final Topic topic) {
		super(true);
		this.topic = topic;
	}

	@Override
	protected final List<UpdateInfo<eu.dnetlib.broker.objects.Dataset>> findUpdates(
		final Pair<Result, List<Dataset>> source,
		final Pair<Result, List<Dataset>> target,
		final DedupConfig dedupConfig) {

		final Set<String> existingDatasets = target
			.getRight()
			.stream()
			.map(Dataset::getId)
			.collect(Collectors.toSet());

		return source
			.getRight()
			.stream()
			.filter(d -> !existingDatasets.contains(d.getId()))
			.map(ConversionUtils::oafDatasetToBrokerDataset)
			.map(i -> generateUpdateInfo(i, source, target, dedupConfig))
			.collect(Collectors.toList());

	}

	protected final UpdateInfo<eu.dnetlib.broker.objects.Dataset> generateUpdateInfo(
		final eu.dnetlib.broker.objects.Dataset highlightValue,
		final Pair<Result, List<Dataset>> source,
		final Pair<Result, List<Dataset>> target,
		final DedupConfig dedupConfig) {
		return new UpdateInfo<>(
			getTopic(),
			highlightValue, source.getLeft(), target.getLeft(),
			(p, rel) -> p.getDatasets().add(rel),
			rel -> rel.getInstances().get(0).getUrl(),
			dedupConfig);
	}

	public Topic getTopic() {
		return topic;
	}
}
