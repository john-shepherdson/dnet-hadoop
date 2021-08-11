
package eu.dnetlib.dhp.broker.oa.matchers.relatedDatasets;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.broker.objects.OaBrokerRelatedDataset;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;

public abstract class AbstractEnrichMissingDataset extends UpdateMatcher<OaBrokerRelatedDataset> {

	protected AbstractEnrichMissingDataset(final Topic topic) {
		super(10,
			rel -> topic,
			(p, rel) -> p.getDatasets().add(rel),
			OaBrokerRelatedDataset::getOpenaireId);
	}

	protected abstract boolean filterByType(String relType);

	@Override
	protected final List<OaBrokerRelatedDataset> findDifferences(final OaBrokerMainEntity source,
		final OaBrokerMainEntity target) {

		if (target.getDatasets().size() >= BrokerConstants.MAX_LIST_SIZE) {
			return new ArrayList<>();
		}

		final Set<String> existingDatasets = target
			.getDatasets()
			.stream()
			.filter(rel -> filterByType(rel.getRelType()))
			.map(OaBrokerRelatedDataset::getOpenaireId)
			.collect(Collectors.toSet());

		return source
			.getDatasets()
			.stream()
			.filter(rel -> filterByType(rel.getRelType()))
			.filter(d -> !existingDatasets.contains(d.getOpenaireId()))
			.collect(Collectors.toList());

	}

}
