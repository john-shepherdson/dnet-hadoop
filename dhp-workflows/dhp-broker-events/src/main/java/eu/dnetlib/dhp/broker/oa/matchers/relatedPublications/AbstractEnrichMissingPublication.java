
package eu.dnetlib.dhp.broker.oa.matchers.relatedPublications;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Result;

public abstract class AbstractEnrichMissingPublication
	extends UpdateMatcher<Pair<Result, List<Publication>>, eu.dnetlib.broker.objects.Publication> {

	private final Topic topic;

	public AbstractEnrichMissingPublication(final Topic topic) {
		super(true);
		this.topic = topic;
	}

	@Override
	protected final List<UpdateInfo<eu.dnetlib.broker.objects.Publication>> findUpdates(
		final Pair<Result, List<Publication>> source,
		final Pair<Result, List<Publication>> target) {

		final Set<String> existingPublications = target
			.getRight()
			.stream()
			.map(Publication::getId)
			.collect(Collectors.toSet());

		return source
			.getRight()
			.stream()
			.filter(d -> !existingPublications.contains(d.getId()))
			.map(ConversionUtils::oafResultToBrokerPublication)
			.map(i -> generateUpdateInfo(i, source, target))
			.collect(Collectors.toList());

	}

	@Override
	protected final UpdateInfo<eu.dnetlib.broker.objects.Publication> generateUpdateInfo(
		final eu.dnetlib.broker.objects.Publication highlightValue,
		final Pair<Result, List<Publication>> source,
		final Pair<Result, List<Publication>> target) {
		return new UpdateInfo<>(
			getTopic(),
			highlightValue, source.getLeft(), target.getLeft(),
			(p, rel) -> p.getPublications().add(rel),
			rel -> rel.getInstances().get(0).getUrl());
	}

	public Topic getTopic() {
		return topic;
	}
}
