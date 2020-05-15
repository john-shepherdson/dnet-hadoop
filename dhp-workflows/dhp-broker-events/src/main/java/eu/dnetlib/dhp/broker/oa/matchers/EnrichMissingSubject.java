
package eu.dnetlib.dhp.broker.oa.matchers;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class EnrichMissingSubject extends UpdateMatcher<Pair<String, String>> {

	public EnrichMissingSubject() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<Pair<String, String>>> findUpdates(final Result source, final Result target) {
		final Set<String> existingTypes = target
			.getSubject()
			.stream()
			.map(StructuredProperty::getQualifier)
			.map(Qualifier::getClassid)
			.collect(Collectors.toSet());

		return source
			.getPid()
			.stream()
			.filter(pid -> !existingTypes.contains(pid.getQualifier().getClassid()))
			.map(ConversionUtils::oafSubjectToPair)
			.map(i -> generateUpdateInfo(i, source, target))
			.collect(Collectors.toList());
	}

	@Override
	public UpdateInfo<Pair<String, String>> generateUpdateInfo(final Pair<String, String> highlightValue,
		final Result source,
		final Result target) {

		return new UpdateInfo<>(
			Topic.fromPath("ENRICH/MISSING/SUBJECT/" + highlightValue.getLeft()),
			highlightValue, source, target,
			(p, pair) -> p.getSubjects().add(pair.getRight()),
			pair -> pair.getLeft() + "::" + pair.getRight());
	}

}
