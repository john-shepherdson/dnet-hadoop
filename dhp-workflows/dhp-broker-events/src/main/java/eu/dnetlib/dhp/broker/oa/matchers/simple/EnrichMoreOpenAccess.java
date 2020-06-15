
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.Instance;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;

public class EnrichMoreOpenAccess extends UpdateMatcher<Instance> {

	public EnrichMoreOpenAccess() {
		super(true,
			i -> Topic.ENRICH_MORE_OA_VERSION,
			(p, i) -> p.getInstances().add(i),
			Instance::getUrl);
	}

	@Override
	protected List<Instance> findDifferences(final ResultWithRelations source,
		final ResultWithRelations target) {
		final Set<String> urls = target
			.getResult()
			.getInstance()
			.stream()
			.filter(i -> i.getAccessright().getClassid().equals(BrokerConstants.OPEN_ACCESS))
			.map(i -> i.getUrl())
			.flatMap(List::stream)
			.collect(Collectors.toSet());

		return source
			.getResult()
			.getInstance()
			.stream()
			.filter(i -> i.getAccessright().getClassid().equals(BrokerConstants.OPEN_ACCESS))
			.map(ConversionUtils::oafInstanceToBrokerInstances)
			.flatMap(List::stream)
			.filter(i -> !urls.contains(i.getUrl()))
			.collect(Collectors.toList());
	}

}
