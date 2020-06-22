
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.OaBrokerInstance;
import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;

public class EnrichMissingOpenAccess extends UpdateMatcher<OaBrokerInstance> {

	public EnrichMissingOpenAccess() {
		super(true,
			i -> Topic.ENRICH_MISSING_OA_VERSION,
			(p, i) -> p.getInstances().add(i),
			OaBrokerInstance::getUrl);
	}

	@Override
	protected List<OaBrokerInstance> findDifferences(final OaBrokerMainEntity source,
		final OaBrokerMainEntity target) {
		final long count = target
			.getInstances()
			.stream()
			.map(OaBrokerInstance::getLicense)
			.filter(right -> right.equals(BrokerConstants.OPEN_ACCESS))
			.count();

		if (count > 0) {
			return Arrays.asList();
		}

		return source
			.getInstances()
			.stream()
			.filter(i -> i.getLicense().equals(BrokerConstants.OPEN_ACCESS))
			.collect(Collectors.toList());
	}

}
