
package eu.dnetlib.dhp.broker.oa.util;

import java.util.Arrays;
import java.util.List;

import eu.dnetlib.broker.objects.Instance;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.schema.oaf.Result;

public class EnrichMoreOpenAccess extends UpdateMatcher<Instance> {

	public EnrichMoreOpenAccess() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<Instance>> findUpdates(final Result source, final Result target) {
		// return Arrays.asList(new EnrichMissingAbstract("xxxxxxx", 0.9f));
		return Arrays.asList();
	}

	@Override
	public UpdateInfo<Instance> generateUpdateInfo(final Instance highlightValue, final Result source,
		final Result target) {
		return new UpdateInfo<>(
			Topic.ENRICH_MORE_OA_VERSION,
			highlightValue, source, target,
			(p, i) -> p.getInstances().add(i),
			Instance::getUrl);
	}

}
