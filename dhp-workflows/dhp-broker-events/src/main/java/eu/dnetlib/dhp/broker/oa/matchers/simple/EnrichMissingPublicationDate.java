
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;

public class EnrichMissingPublicationDate extends UpdateMatcher<String> {

	public EnrichMissingPublicationDate() {
		super(1,
			date -> Topic.ENRICH_MISSING_PUBLICATION_DATE,
			(p, date) -> p.setPublicationdate(date),
			s -> s);
	}

	@Override
	protected List<String> findDifferences(final OaBrokerMainEntity source,
		final OaBrokerMainEntity target) {

		if (isMissing(target.getPublicationdate()) && !isMissing(source.getPublicationdate())) {
			return Arrays.asList(source.getPublicationdate());
		} else {
			return new ArrayList<>();
		}
	}

}
