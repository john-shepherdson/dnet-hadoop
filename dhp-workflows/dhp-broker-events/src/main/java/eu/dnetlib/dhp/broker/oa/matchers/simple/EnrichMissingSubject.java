
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.pace.config.DedupConfig;

public class EnrichMissingSubject extends UpdateMatcher<Result, Pair<String, String>> {

	public EnrichMissingSubject() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<Pair<String, String>>> findUpdates(final Result source, final Result target, final DedupConfig dedupConfig) {
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
			.map(i -> generateUpdateInfo(i, source, target, dedupConfig))
			.collect(Collectors.toList());
	}

	public UpdateInfo<Pair<String, String>> generateUpdateInfo(final Pair<String, String> highlightValue,
		final Result source,
		final Result target,
		final DedupConfig dedupConfig) {

		return new UpdateInfo<>(
			Topic.fromPath("ENRICH/MISSING/SUBJECT/" + highlightValue.getLeft()),
			highlightValue, source, target,
			(p, pair) -> p.getSubjects().add(pair.getRight()),
			pair -> pair.getLeft() + "::" + pair.getRight(), dedupConfig);
	}

}
