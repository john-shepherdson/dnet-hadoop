
package eu.dnetlib.dhp.broker.oa.matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.schema.oaf.Result;

public class EnrichMissingAbstract extends UpdateMatcher<String> {

	public EnrichMissingAbstract() {
		super(false);
	}

	@Override
	protected List<UpdateInfo<String>> findUpdates(final Result source, final Result target) {
		if (isMissing(target.getDescription()) && !isMissing(source.getDescription())) {
			return Arrays.asList(generateUpdateInfo(source.getDescription().get(0).getValue(), source, target));
		}
		return new ArrayList<>();
	}

	@Override
	public UpdateInfo<String> generateUpdateInfo(final String highlightValue, final Result source,
		final Result target) {
		return new UpdateInfo<>(
			Topic.ENRICH_MISSING_ABSTRACT,
			highlightValue, source, target,
			(p, s) -> p.getAbstracts().add(s),
			s -> s);
	}

}
