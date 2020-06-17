
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.OpenaireBrokerResult;
import eu.dnetlib.broker.objects.TypedValue;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;

public class EnrichMissingSubject extends UpdateMatcher<TypedValue> {

	public EnrichMissingSubject() {
		super(true,
			s -> Topic.fromPath("ENRICH/MISSING/SUBJECT/" + s.getType()),
			(p, s) -> p.getSubjects().add(s),
			s -> subjectAsString(s));
	}

	@Override
	protected List<TypedValue> findDifferences(final OpenaireBrokerResult source,
		final OpenaireBrokerResult target) {
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

	private static String subjectAsString(final TypedValue s) {
		return s.getType() + "::" + s.getValue();
	}

}
