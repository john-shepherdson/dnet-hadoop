
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.OaBrokerInstance;
import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;

public class EnrichMoreOpenAccess extends UpdateMatcher<OaBrokerInstance> {

	public EnrichMoreOpenAccess() {
		super(20,
			i -> Topic.ENRICH_MORE_OA_VERSION,
			(p, i) -> p.getInstances().add(i),
			OaBrokerInstance::getUrl);
	}

	@Override
	protected List<OaBrokerInstance> findDifferences(final OaBrokerMainEntity source,
		final OaBrokerMainEntity target) {

		if (target.getInstances().size() >= BrokerConstants.MAX_LIST_SIZE) {
			return new ArrayList<>();
		}

		final Set<String> urls = target
			.getInstances()
			.stream()
			.filter(i -> i.getLicense().equals(BrokerConstants.OPEN_ACCESS))
			.map(i -> i.getUrl())
			.collect(Collectors.toSet());

		return source
			.getInstances()
			.stream()
			.filter(i -> i.getLicense().equals(BrokerConstants.OPEN_ACCESS))
			.filter(i -> !urls.contains(i.getUrl()))
			.collect(Collectors.toList());
	}

}
