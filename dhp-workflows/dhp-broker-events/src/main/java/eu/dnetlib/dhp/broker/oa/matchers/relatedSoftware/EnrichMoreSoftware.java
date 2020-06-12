
package eu.dnetlib.dhp.broker.oa.matchers.relatedSoftware;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedSoftware;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;
import eu.dnetlib.dhp.schema.oaf.Software;

public class EnrichMoreSoftware
	extends UpdateMatcher<eu.dnetlib.broker.objects.Software> {

	public EnrichMoreSoftware() {
		super(true,
			s -> Topic.ENRICH_MORE_SOFTWARE,
			(p, s) -> p.getSoftwares().add(s),
			s -> s.getName());
	}

	@Override
	protected List<eu.dnetlib.broker.objects.Software> findDifferences(
		final ResultWithRelations source,
		final ResultWithRelations target) {

		final Set<String> existingSoftwares = source
			.getSoftwares()
			.stream()
			.map(RelatedSoftware::getRelSoftware)
			.map(Software::getId)
			.collect(Collectors.toSet());

		return target
			.getSoftwares()
			.stream()
			.map(RelatedSoftware::getRelSoftware)
			.filter(p -> !existingSoftwares.contains(p.getId()))
			.map(ConversionUtils::oafSoftwareToBrokerSoftware)
			.collect(Collectors.toList());
	}

}
