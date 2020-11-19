
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;

public class EnrichMissingAbstract extends UpdateMatcher<String> {

	private static final int MIN_LENGTH = 200;

	public EnrichMissingAbstract() {
		super(1,
			s -> Topic.ENRICH_MISSING_ABSTRACT,
			(p, s) -> p.getAbstracts().add(s),
			s -> s);
	}

	@Override
	protected List<String> findDifferences(final OaBrokerMainEntity source, final OaBrokerMainEntity target) {
		if (isMissing(target.getAbstracts()) && !isMissing(source.getAbstracts())) {
			return source
				.getAbstracts()
				.stream()
				.filter(s -> StringUtils.normalizeSpace(s).length() >= MIN_LENGTH)
				.map(Arrays::asList)
				.findFirst()
				.orElse(new ArrayList<>());
		}
		return new ArrayList<>();
	}

}
