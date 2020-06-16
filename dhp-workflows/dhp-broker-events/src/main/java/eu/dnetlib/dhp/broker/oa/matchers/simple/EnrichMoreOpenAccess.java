
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.Instance;
import eu.dnetlib.broker.objects.OpenaireBrokerResult;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;

public class EnrichMoreOpenAccess extends UpdateMatcher<Instance> {

	public EnrichMoreOpenAccess() {
		super(true,
			i -> Topic.ENRICH_MORE_OA_VERSION,
			(p, i) -> p.getInstances().add(i),
			Instance::getUrl);
	}

	@Override
	protected List<Instance> findDifferences(final OpenaireBrokerResult source,
		final OpenaireBrokerResult target) {
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
