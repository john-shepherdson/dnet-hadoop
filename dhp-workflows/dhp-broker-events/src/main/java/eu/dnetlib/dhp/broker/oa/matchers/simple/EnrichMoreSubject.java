
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.schema.oaf.Result;

public class EnrichMoreSubject extends UpdateMatcher<Result, Pair<String, String>> {

	public EnrichMoreSubject() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<Pair<String, String>>> findUpdates(final Result source, final Result target) {
		final Set<String> existingSubjects = target
			.getSubject()
			.stream()
			.map(pid -> pid.getQualifier().getClassid() + "::" + pid.getValue())
			.collect(Collectors.toSet());

		return source
			.getPid()
			.stream()
			.filter(pid -> !existingSubjects.contains(pid.getQualifier().getClassid() + "::" + pid.getValue()))
			.map(ConversionUtils::oafSubjectToPair)
			.map(i -> generateUpdateInfo(i, source, target))
			.collect(Collectors.toList());
	}

	@Override
	public UpdateInfo<Pair<String, String>> generateUpdateInfo(final Pair<String, String> highlightValue,
		final Result source,
		final Result target) {

		return new UpdateInfo<>(
			Topic.fromPath("ENRICH/MORE/SUBJECT/" + highlightValue.getLeft()),
			highlightValue, source, target,
			(p, pair) -> p.getSubjects().add(pair.getRight()),
			pair -> pair.getLeft() + "::" + pair.getRight());
	}

}
