
package eu.dnetlib.dhp.broker.oa.matchers.relatedSoftware;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.OpenaireBrokerResult;
import eu.dnetlib.broker.objects.Software;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;

public class EnrichMoreSoftware extends UpdateMatcher<Software> {

	public EnrichMoreSoftware() {
		super(true,
			s -> Topic.ENRICH_MORE_SOFTWARE,
			(p, s) -> p.getSoftwares().add(s),
			s -> s.getName());
	}

	@Override
	protected List<eu.dnetlib.broker.objects.Software> findDifferences(
		final OpenaireBrokerResult source,
		final OpenaireBrokerResult target) {

		final Set<String> existingSoftwares = source
			.getSoftwares()
			.stream()
			.map(Software::getName)
			.collect(Collectors.toSet());

		return target
			.getSoftwares()
			.stream()
			.filter(p -> !existingSoftwares.contains(p.getName()))
			.collect(Collectors.toList());
	}

}
