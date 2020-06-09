
package eu.dnetlib.dhp.broker.oa.matchers.relatedSoftware;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Software;
import eu.dnetlib.pace.config.DedupConfig;

public class EnrichMoreSoftware
	extends UpdateMatcher<Pair<Result, List<Software>>, eu.dnetlib.broker.objects.Software> {

	public EnrichMoreSoftware() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<eu.dnetlib.broker.objects.Software>> findUpdates(
		final Pair<Result, List<Software>> source,
		final Pair<Result, List<Software>> target,
		final DedupConfig dedupConfig) {

		final Set<String> existingSoftwares = source
			.getRight()
			.stream()
			.map(Software::getId)
			.collect(Collectors.toSet());

		return target
			.getRight()
			.stream()
			.filter(p -> !existingSoftwares.contains(p.getId()))
			.map(ConversionUtils::oafSoftwareToBrokerSoftware)
			.map(p -> generateUpdateInfo(p, source, target, dedupConfig))
			.collect(Collectors.toList());
	}

	public UpdateInfo<eu.dnetlib.broker.objects.Software> generateUpdateInfo(
		final eu.dnetlib.broker.objects.Software highlightValue,
		final Pair<Result, List<Software>> source,
		final Pair<Result, List<Software>> target,
		final DedupConfig dedupConfig) {
		return new UpdateInfo<>(
			Topic.ENRICH_MORE_SOFTWARE,
			highlightValue, source.getLeft(), target.getLeft(),
			(p, s) -> p.getSoftwares().add(s),
			s -> s.getName(), dedupConfig);
	}

}
