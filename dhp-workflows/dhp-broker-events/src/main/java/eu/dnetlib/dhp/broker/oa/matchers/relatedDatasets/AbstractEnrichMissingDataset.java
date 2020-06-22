
package eu.dnetlib.dhp.broker.oa.matchers.relatedDatasets;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.broker.objects.OaBrokerRelatedDataset;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;

public abstract class AbstractEnrichMissingDataset extends UpdateMatcher<OaBrokerRelatedDataset> {

	public AbstractEnrichMissingDataset(final Topic topic) {
		super(true,
			rel -> topic,
			(p, rel) -> p.getDatasets().add(rel),
			rel -> rel.getOriginalId());
	}

	protected abstract boolean filterByType(String relType);

	@Override
	protected final List<OaBrokerRelatedDataset> findDifferences(final OaBrokerMainEntity source,
		final OaBrokerMainEntity target) {

		final Set<String> existingDatasets = target
			.getDatasets()
			.stream()
			.filter(rel -> filterByType(rel.getRelType()))
			.map(OaBrokerRelatedDataset::getOriginalId)
			.collect(Collectors.toSet());

		return source
			.getDatasets()
			.stream()
			.filter(rel -> filterByType(rel.getRelType()))
			.filter(d -> !existingDatasets.contains(d.getOriginalId()))
			.collect(Collectors.toList());

	}

}
