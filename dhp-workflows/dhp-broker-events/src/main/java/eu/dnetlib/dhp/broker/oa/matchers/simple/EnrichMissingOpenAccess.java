
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
import eu.dnetlib.dhp.schema.oaf.Result;

public class EnrichMissingOpenAccess extends UpdateMatcher<Result, Instance> {

	public EnrichMissingOpenAccess() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<Instance>> findUpdates(final Result source, final Result target) {
		final long count = target
			.getInstance()
			.stream()
			.map(i -> i.getAccessright().getClassid())
			.filter(right -> right.equals(BrokerConstants.OPEN_ACCESS))
			.count();

		if (count > 0) {
			return Arrays.asList();
		}

		return source
			.getInstance()
			.stream()
			.filter(i -> i.getAccessright().getClassid().equals(BrokerConstants.OPEN_ACCESS))
			.map(ConversionUtils::oafInstanceToBrokerInstances)
			.flatMap(s -> s)
			.map(i -> generateUpdateInfo(i, source, target))
			.collect(Collectors.toList());
	}

	@Override
	public UpdateInfo<Instance> generateUpdateInfo(final Instance highlightValue,
		final Result source,
		final Result target) {
		return new UpdateInfo<>(
			Topic.ENRICH_MISSING_OA_VERSION,
			highlightValue, source, target,
			(p, i) -> p.getInstances().add(i),
			Instance::getUrl);
	}

}
