
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;

public class EnrichMissingPublicationDate extends UpdateMatcher<String> {

	public EnrichMissingPublicationDate() {
		super(false,
			date -> Topic.ENRICH_MISSING_PUBLICATION_DATE,
			(p, date) -> p.setPublicationdate(date),
			s -> s);
	}

	@Override
	protected List<String> findDifferences(final ResultWithRelations source,
		final ResultWithRelations target) {
		if (isMissing(target.getResult().getDateofacceptance())
			&& !isMissing(source.getResult().getDateofacceptance())) {
			return Arrays.asList(source.getResult().getDateofacceptance().getValue());
		}
		return new ArrayList<>();
	}

}
