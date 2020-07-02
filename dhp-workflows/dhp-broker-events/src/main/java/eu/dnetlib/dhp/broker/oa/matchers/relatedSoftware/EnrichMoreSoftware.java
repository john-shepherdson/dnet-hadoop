
package eu.dnetlib.dhp.broker.oa.matchers.relatedSoftware;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.broker.objects.OaBrokerRelatedSoftware;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;

public class EnrichMoreSoftware extends UpdateMatcher<OaBrokerRelatedSoftware> {

	public EnrichMoreSoftware() {
		super(10,
			s -> Topic.ENRICH_MORE_SOFTWARE,
			(p, s) -> p.getSoftwares().add(s),
			s -> s.getOpenaireId());
	}

	@Override
	protected List<OaBrokerRelatedSoftware> findDifferences(
		final OaBrokerMainEntity source,
		final OaBrokerMainEntity target) {

		if (target.getSoftwares().size() >= BrokerConstants.MAX_LIST_SIZE) {
			return new ArrayList<>();
		}

		final Set<String> existingSoftwares = source
			.getSoftwares()
			.stream()
			.map(OaBrokerRelatedSoftware::getName)
			.collect(Collectors.toSet());

		return target
			.getSoftwares()
			.stream()
			.filter(p -> !existingSoftwares.contains(p.getName()))
			.collect(Collectors.toList());
	}

}
