
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class EnrichMissingSubject extends UpdateMatcher<Pair<String, String>> {

	public EnrichMissingSubject() {
		super(true,
			pair -> Topic.fromPath("ENRICH/MISSING/SUBJECT/" + pair.getLeft()),
			(p, pair) -> p.getSubjects().add(pair.getRight()),
			pair -> pair.getLeft() + "::" + pair.getRight());
	}

	@Override
	protected List<Pair<String, String>> findDifferences(final ResultWithRelations source,
		final ResultWithRelations target) {
		final Set<String> existingTypes = target
			.getResult()
			.getSubject()
			.stream()
			.map(StructuredProperty::getQualifier)
			.map(Qualifier::getClassid)
			.collect(Collectors.toSet());

		return source
			.getResult()
			.getPid()
			.stream()
			.filter(pid -> !existingTypes.contains(pid.getQualifier().getClassid()))
			.map(ConversionUtils::oafSubjectToPair)
			.collect(Collectors.toList());
	}

}
