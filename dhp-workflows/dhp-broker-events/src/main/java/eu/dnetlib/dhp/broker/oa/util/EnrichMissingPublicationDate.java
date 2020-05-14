
package eu.dnetlib.dhp.broker.oa.util;

import java.util.Arrays;
import java.util.List;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.schema.oaf.Result;

public class EnrichMissingPublicationDate extends UpdateMatcher<String> {

	public EnrichMissingPublicationDate() {
		super(false);
	}

	@Override
	protected List<UpdateInfo<String>> findUpdates(final Result source, final Result target) {
		// return Arrays.asList(new EnrichMissingAbstract("xxxxxxx", 0.9f));
		return Arrays.asList();
	}

	@Override
	public UpdateInfo<String> generateUpdateInfo(final String highlightValue, final Result source,
		final Result target) {
		return new UpdateInfo<>(
			Topic.ENRICH_MISSING_PUBLICATION_DATE,
			highlightValue, source, target,
			(p, date) -> p.setPublicationdate(date),
			s -> s);
	}

}
