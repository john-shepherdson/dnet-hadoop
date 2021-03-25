
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.broker.objects.OaBrokerTypedValue;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;

public class EnrichMissingSubject extends UpdateMatcher<OaBrokerTypedValue> {

	public EnrichMissingSubject() {
		super(20,
			s -> {
				switch (s.getType().toLowerCase()) {
					case "acm":
						return Topic.ENRICH_MISSING_SUBJECT_ACM;
					case "arxiv":
						return Topic.ENRICH_MISSING_SUBJECT_ARXIV;
					case "ddc":
						return Topic.ENRICH_MISSING_SUBJECT_DDC;
					case "jel":
						return Topic.ENRICH_MISSING_SUBJECT_JEL;
					case "mesh":
						return Topic.ENRICH_MISSING_SUBJECT_MESHEUROPMC;
					case "rvk":
						return Topic.ENRICH_MISSING_SUBJECT_RVK;
					default:
						return null;
				}
			},
			(p, s) -> p.getSubjects().add(s),
			s -> subjectAsString(s));
	}

	@Override
	protected List<OaBrokerTypedValue> findDifferences(final OaBrokerMainEntity source,
		final OaBrokerMainEntity target) {

		if (target.getSubjects().size() >= BrokerConstants.MAX_LIST_SIZE) {
			return new ArrayList<>();
		}

		final Set<String> existingSubject = target
			.getSubjects()
			.stream()
			.map(s -> subjectAsString(s))
			.collect(Collectors.toSet());

		return source
			.getSubjects()
			.stream()
			.filter(s -> !existingSubject.contains(subjectAsString(s)))
			.collect(Collectors.toList());
	}

	private static String subjectAsString(final OaBrokerTypedValue s) {
		return s.getType() + "::" + s.getValue();
	}

}
