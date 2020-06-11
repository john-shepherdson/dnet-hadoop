
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.Instance;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;
import eu.dnetlib.pace.config.DedupConfig;

public class EnrichMissingOpenAccess extends UpdateMatcher<Instance> {

	public EnrichMissingOpenAccess() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<Instance>> findUpdates(final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {
		final long count = target
			.getResult()
			.getInstance()
			.stream()
			.map(i -> i.getAccessright().getClassid())
			.filter(right -> right.equals(BrokerConstants.OPEN_ACCESS))
			.count();

		if (count > 0) {
			return Arrays.asList();
		}

		return source
			.getResult()
			.getInstance()
			.stream()
			.filter(i -> i.getAccessright().getClassid().equals(BrokerConstants.OPEN_ACCESS))
			.map(ConversionUtils::oafInstanceToBrokerInstances)
			.flatMap(List::stream)
			.map(i -> generateUpdateInfo(i, source, target, dedupConfig))
			.collect(Collectors.toList());
	}

	public UpdateInfo<Instance> generateUpdateInfo(final Instance highlightValue,
		final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {
		return new UpdateInfo<>(
			Topic.ENRICH_MISSING_OA_VERSION,
			highlightValue, source, target,
			(p, i) -> p.getInstances().add(i),
			Instance::getUrl, dedupConfig);
	}

}
