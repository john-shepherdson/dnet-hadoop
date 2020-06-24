
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;

public class EnrichMissingAbstract extends UpdateMatcher<String> {

	public EnrichMissingAbstract() {
		super(false,
			s -> Topic.ENRICH_MISSING_ABSTRACT,
			(p, s) -> p.getAbstracts().add(s),
			s -> s);
	}

	@Override
	protected List<String> findDifferences(final OaBrokerMainEntity source, final OaBrokerMainEntity target) {
		if (isMissing(target.getAbstracts()) && !isMissing(source.getAbstracts())) {
			return Arrays.asList(source.getAbstracts().get(0));
		} else {
			return new ArrayList<>();
		}
	}

}
