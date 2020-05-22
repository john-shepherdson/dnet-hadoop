
package eu.dnetlib.dhp.broker.oa.matchers;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Result;

public class EnrichMissingDatasetReferences
	extends UpdateMatcher<Pair<Result, List<Dataset>>, eu.dnetlib.broker.objects.Dataset> {

	public EnrichMissingDatasetReferences() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<eu.dnetlib.broker.objects.Dataset>> findUpdates(final Pair<Result, List<Dataset>> source,
		final Pair<Result, List<Dataset>> target) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected UpdateInfo<eu.dnetlib.broker.objects.Dataset> generateUpdateInfo(
		final eu.dnetlib.broker.objects.Dataset highlightValue,
		final Pair<Result, List<Dataset>> source,
		final Pair<Result, List<Dataset>> target) {
		return new UpdateInfo<>(
			Topic.ENRICH_MISSING_DATASET_REFERENCES,
			highlightValue, source.getLeft(), target.getLeft(),
			(p, rel) -> p.getDatasets().add(rel),
			rel -> rel.getInstances().get(0).getUrl());
	}
}
