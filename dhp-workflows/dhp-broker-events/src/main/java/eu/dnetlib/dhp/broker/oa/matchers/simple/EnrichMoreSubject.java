
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;

public class EnrichMoreSubject extends UpdateMatcher<Pair<String, String>> {

	public EnrichMoreSubject() {
		super(true,
			pair -> Topic.fromPath("ENRICH/MORE/SUBJECT/" + pair.getLeft()),
			(p, pair) -> p.getSubjects().add(pair.getRight()),
			pair -> pair.getLeft() + "::" + pair.getRight());
	}

	@Override
	protected List<Pair<String, String>> findDifferences(final ResultWithRelations source,
		final ResultWithRelations target) {
		final Set<String> existingSubjects = target
			.getResult()
			.getSubject()
			.stream()
			.map(pid -> pid.getQualifier().getClassid() + "::" + pid.getValue())
			.collect(Collectors.toSet());

		return source
			.getResult()
			.getPid()
			.stream()
			.filter(pid -> !existingSubjects.contains(pid.getQualifier().getClassid() + "::" + pid.getValue()))
			.map(ConversionUtils::oafSubjectToPair)
			.collect(Collectors.toList());
	}

}
