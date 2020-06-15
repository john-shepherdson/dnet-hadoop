
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;

public class EnrichMissingAbstract extends UpdateMatcher<String> {

	public EnrichMissingAbstract() {
		super(false,
			s -> Topic.ENRICH_MISSING_ABSTRACT,
			(p, s) -> p.getAbstracts().add(s),
			s -> s);
	}

	@Override
	protected List<String> findDifferences(final ResultWithRelations source,
		final ResultWithRelations target) {
		if (isMissing(target.getResult().getDescription()) && !isMissing(source.getResult().getDescription())) {
			return Arrays
				.asList(source.getResult().getDescription().get(0).getValue());
		}
		return new ArrayList<>();
	}

}
