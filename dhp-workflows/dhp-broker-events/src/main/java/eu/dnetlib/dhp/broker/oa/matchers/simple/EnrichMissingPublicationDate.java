
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.pace.config.DedupConfig;

public class EnrichMissingPublicationDate extends UpdateMatcher<Result, String> {

	public EnrichMissingPublicationDate() {
		super(false);
	}

	@Override
	protected List<UpdateInfo<String>> findUpdates(final Result source, final Result target, final DedupConfig dedupConfig) {
		if (isMissing(target.getDateofacceptance()) && !isMissing(source.getDateofacceptance())) {
			return Arrays.asList(generateUpdateInfo(source.getDateofacceptance().getValue(), source, target, dedupConfig));
		}
		return new ArrayList<>();
	}

	public UpdateInfo<String> generateUpdateInfo(final String highlightValue,
		final Result source,
		final Result target,
		final DedupConfig dedupConfig) {
		return new UpdateInfo<>(
			Topic.ENRICH_MISSING_PUBLICATION_DATE,
			highlightValue, source, target,
			(p, date) -> p.setPublicationdate(date),
			s -> s, dedupConfig);
	}

}
