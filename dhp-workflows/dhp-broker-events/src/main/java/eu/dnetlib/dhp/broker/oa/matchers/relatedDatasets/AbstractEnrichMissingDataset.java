
package eu.dnetlib.dhp.broker.oa.matchers.relatedDatasets;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedDataset;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;
import eu.dnetlib.dhp.schema.oaf.Dataset;

public abstract class AbstractEnrichMissingDataset
	extends UpdateMatcher<eu.dnetlib.broker.objects.Dataset> {

	public AbstractEnrichMissingDataset(final Topic topic) {
		super(true,
			rel -> topic,
			(p, rel) -> p.getDatasets().add(rel),
			rel -> rel.getInstances().get(0).getUrl());
	}

	protected abstract boolean filterByType(String relType);

	@Override
	protected final List<eu.dnetlib.broker.objects.Dataset> findDifferences(
		final ResultWithRelations source,
		final ResultWithRelations target) {

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
			.collect(Collectors.toList());

	}

}
