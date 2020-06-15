
package eu.dnetlib.dhp.broker.oa.matchers.relatedPublications;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedPublication;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;
import eu.dnetlib.dhp.schema.oaf.Publication;

public abstract class AbstractEnrichMissingPublication
	extends UpdateMatcher<eu.dnetlib.broker.objects.Publication> {

	public AbstractEnrichMissingPublication(final Topic topic) {
		super(true,
			rel -> topic,
			(p, rel) -> p.getPublications().add(rel),
			rel -> rel.getInstances().get(0).getUrl());

	}

	protected abstract boolean filterByType(String relType);

	@Override
	protected final List<eu.dnetlib.broker.objects.Publication> findDifferences(
		final ResultWithRelations source,
		final ResultWithRelations target) {

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
			.collect(Collectors.toList());
	}

}
