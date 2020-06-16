
package eu.dnetlib.dhp.broker.oa.matchers.relatedPublications;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.OpenaireBrokerResult;
import eu.dnetlib.broker.objects.Publication;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;

public abstract class AbstractEnrichMissingPublication extends UpdateMatcher<Publication> {

	public AbstractEnrichMissingPublication(final Topic topic) {
		super(true,
			rel -> topic,
			(p, rel) -> p.getPublications().add(rel),
			rel -> rel.getOriginalId());

	}

	protected abstract boolean filterByType(String relType);

	@Override
	protected final List<eu.dnetlib.broker.objects.Publication> findDifferences(
		final OpenaireBrokerResult source,
		final OpenaireBrokerResult target) {

		final Set<String> existingPublications = target
			.getPublications()
			.stream()
			.filter(rel -> filterByType(rel.getRelType()))
			.map(Publication::getOriginalId)
			.collect(Collectors.toSet());

		return source
			.getPublications()
			.stream()
			.filter(rel -> filterByType(rel.getRelType()))
			.filter(p -> !existingPublications.contains(p.getOriginalId()))
			.collect(Collectors.toList());
	}

}
