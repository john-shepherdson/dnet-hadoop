
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.Instance;
import eu.dnetlib.broker.objects.OpenaireBrokerResult;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;

public class EnrichMissingOpenAccess extends UpdateMatcher<Instance> {

	public EnrichMissingOpenAccess() {
		super(true,
			i -> Topic.ENRICH_MISSING_OA_VERSION,
			(p, i) -> p.getInstances().add(i),
			Instance::getUrl);
	}

	@Override
	protected List<Instance> findDifferences(final OpenaireBrokerResult source,
		final OpenaireBrokerResult target) {
		final long count = target
			.getInstances()
			.stream()
			.map(Instance::getLicense)
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
