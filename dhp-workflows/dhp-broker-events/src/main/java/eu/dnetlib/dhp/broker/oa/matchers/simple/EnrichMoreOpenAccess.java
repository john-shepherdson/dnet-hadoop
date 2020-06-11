
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.Instance;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;
import eu.dnetlib.pace.config.DedupConfig;

public class EnrichMoreOpenAccess extends UpdateMatcher<Instance> {

	public EnrichMoreOpenAccess() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<Instance>> findUpdates(final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {
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
			.map(i -> generateUpdateInfo(i, source, target, dedupConfig))
			.collect(Collectors.toList());
	}

	public UpdateInfo<Instance> generateUpdateInfo(final Instance highlightValue,
		final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {
		return new UpdateInfo<>(
			Topic.ENRICH_MORE_OA_VERSION,
			highlightValue, source, target,
			(p, i) -> p.getInstances().add(i),
			Instance::getUrl, dedupConfig);
	}

}
