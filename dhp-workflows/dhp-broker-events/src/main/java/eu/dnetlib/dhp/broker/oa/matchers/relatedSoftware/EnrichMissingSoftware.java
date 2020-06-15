
package eu.dnetlib.dhp.broker.oa.matchers.relatedSoftware;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedSoftware;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;

public class EnrichMissingSoftware
	extends UpdateMatcher<eu.dnetlib.broker.objects.Software> {

	public EnrichMissingSoftware() {
		super(true,
			s -> Topic.ENRICH_MISSING_SOFTWARE,
			(p, s) -> p.getSoftwares().add(s),
			s -> s.getName());
	}

	@Override
	protected List<eu.dnetlib.broker.objects.Software> findDifferences(
		final ResultWithRelations source,
		final ResultWithRelations target) {

		if (source.getSoftwares().isEmpty()) {
			return Arrays.asList();
		} else {
			return target
				.getSoftwares()
				.stream()
				.map(RelatedSoftware::getRelSoftware)
				.map(ConversionUtils::oafSoftwareToBrokerSoftware)
				.collect(Collectors.toList());
		}
	}

}
