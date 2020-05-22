
package eu.dnetlib.dhp.broker.oa.matchers;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Result;

public class EnrichMissingPublicationIsReferencedBy
	extends UpdateMatcher<Pair<Result, List<Publication>>, eu.dnetlib.broker.objects.Publication> {

	public EnrichMissingPublicationIsReferencedBy() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<eu.dnetlib.broker.objects.Publication>> findUpdates(
		final Pair<Result, List<Publication>> source,
		final Pair<Result, List<Publication>> target) {
		// TODO Auto-generated method stub
		return Arrays.asList();
	}

	@Override
	protected UpdateInfo<eu.dnetlib.broker.objects.Publication> generateUpdateInfo(
		final eu.dnetlib.broker.objects.Publication highlightValue,
		final Pair<Result, List<Publication>> source,
		final Pair<Result, List<Publication>> target) {
		return new UpdateInfo<>(
			Topic.ENRICH_MISSING_PUBLICATION_IS_REFERENCED_BY,
			highlightValue, source.getLeft(), target.getLeft(),
			(p, rel) -> {
			}, // p.getPublications().add(rel), //TODO available in the future release of dnet-openaire-broker-common
			rel -> rel.getOriginalId());
	}

}
