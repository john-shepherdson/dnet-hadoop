
package eu.dnetlib.dhp.broker.oa.util;

import java.util.Arrays;
import java.util.List;

import eu.dnetlib.broker.objects.Instance;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.schema.oaf.Result;

public class EnrichMissingOpenAccess extends UpdateMatcher<Instance> {

	public EnrichMissingOpenAccess() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<Instance>> findUpdates(final Result source, final Result target) {

		return Arrays.asList();
	}

	@Override
	public UpdateInfo<Instance> generateUpdateInfo(final Instance highlightValue, final Result source,
		final Result target) {
		return new UpdateInfo<>(
			Topic.ENRICH_MISSING_OA_VERSION,
			highlightValue, source, target,
			(p, i) -> p.getInstances().add(i),
			Instance::getUrl);
	}

}
