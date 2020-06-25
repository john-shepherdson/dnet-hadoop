
package eu.dnetlib.dhp.broker.oa.matchers.relatedPublications;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.broker.objects.OaBrokerRelatedPublication;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;

public abstract class AbstractEnrichMissingPublication extends UpdateMatcher<OaBrokerRelatedPublication> {

	public AbstractEnrichMissingPublication(final Topic topic) {
		super(true,
			rel -> topic,
			(p, rel) -> p.getPublications().add(rel),
			rel -> rel.getOpenaireId());

	}

	protected abstract boolean filterByType(String relType);

	@Override
	protected final List<OaBrokerRelatedPublication> findDifferences(
		final OaBrokerMainEntity source,
		final OaBrokerMainEntity target) {

		final Set<String> existingPublications = target
			.getPublications()
			.stream()
			.filter(rel -> filterByType(rel.getRelType()))
			.map(OaBrokerRelatedPublication::getOpenaireId)
			.collect(Collectors.toSet());

		return source
			.getPublications()
			.stream()
			.filter(rel -> filterByType(rel.getRelType()))
			.filter(p -> !existingPublications.contains(p.getOpenaireId()))
			.collect(Collectors.toList());
	}

}
