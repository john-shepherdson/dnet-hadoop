
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.OpenaireBrokerResult;
import eu.dnetlib.broker.objects.TypedValue;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;

public class EnrichMoreSubject extends UpdateMatcher<TypedValue> {

	public EnrichMoreSubject() {
		super(true,
			s -> Topic.fromPath("ENRICH/MORE/SUBJECT/" + s.getType()),
			(p, s) -> p.getSubjects().add(s),
			s -> subjectAsString(s));
	}

	@Override
	protected List<TypedValue> findDifferences(final OpenaireBrokerResult source,
		final OpenaireBrokerResult target) {
		final Set<String> existingSubjects = target
			.getSubjects()
			.stream()
			.map(pid -> subjectAsString(pid))
			.collect(Collectors.toSet());

		return source
			.getPids()
			.stream()
			.filter(s -> !existingSubjects.contains(subjectAsString(s)))
			.collect(Collectors.toList());
	}

	private static String subjectAsString(final TypedValue s) {
		return s.getType() + "::" + s.getValue();
	}
}
