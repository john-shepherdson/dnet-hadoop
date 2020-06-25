
package eu.dnetlib.dhp.broker.oa.matchers.relatedSoftware;

import java.util.ArrayList;
import java.util.List;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.broker.objects.OaBrokerRelatedSoftware;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;

public class EnrichMissingSoftware
	extends UpdateMatcher<OaBrokerRelatedSoftware> {

	public EnrichMissingSoftware() {
		super(true,
			s -> Topic.ENRICH_MISSING_SOFTWARE,
			(p, s) -> p.getSoftwares().add(s),
			s -> s.getOpenaireId());
	}

	@Override
	protected List<OaBrokerRelatedSoftware> findDifferences(
		final OaBrokerMainEntity source,
		final OaBrokerMainEntity target) {

		if (target.getSoftwares().isEmpty()) {
			return source.getSoftwares();
		} else {
			return new ArrayList<>();
		}
	}

}
