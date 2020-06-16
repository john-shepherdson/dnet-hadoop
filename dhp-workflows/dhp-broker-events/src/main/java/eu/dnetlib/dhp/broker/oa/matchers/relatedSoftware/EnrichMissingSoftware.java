
package eu.dnetlib.dhp.broker.oa.matchers.relatedSoftware;

import java.util.ArrayList;
import java.util.List;

import eu.dnetlib.broker.objects.OpenaireBrokerResult;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;

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
		final OpenaireBrokerResult source,
		final OpenaireBrokerResult target) {

		if (target.getSoftwares().isEmpty()) {
			return source.getSoftwares();
		} else {
			return new ArrayList<>();
		}
	}

}
