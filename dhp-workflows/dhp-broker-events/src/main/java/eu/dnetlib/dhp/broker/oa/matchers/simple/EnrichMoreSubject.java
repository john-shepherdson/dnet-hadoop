
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;
import eu.dnetlib.pace.config.DedupConfig;

public class EnrichMoreSubject extends UpdateMatcher<Pair<String, String>> {

	public EnrichMoreSubject() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<Pair<String, String>>> findUpdates(final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {
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
			.map(i -> generateUpdateInfo(i, source, target, dedupConfig))
			.collect(Collectors.toList());
	}

	public UpdateInfo<Pair<String, String>> generateUpdateInfo(final Pair<String, String> highlightValue,
		final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {

		return new UpdateInfo<>(
			Topic.fromPath("ENRICH/MORE/SUBJECT/" + highlightValue.getLeft()),
			highlightValue, source, target,
			(p, pair) -> p.getSubjects().add(pair.getRight()),
			pair -> pair.getLeft() + "::" + pair.getRight(), dedupConfig);
	}

}
