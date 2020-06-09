
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.pace.config.DedupConfig;

public class EnrichMissingAuthorOrcid extends UpdateMatcher<Result, Pair<String, String>> {

	public EnrichMissingAuthorOrcid() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<Pair<String, String>>> findUpdates(final Result source, final Result target, final DedupConfig dedupConfig) {
		// TODO
		// return Arrays.asList(new EnrichMissingAbstract("xxxxxxx", 0.9f));
		return Arrays.asList();
	}

	public UpdateInfo<Pair<String, String>> generateUpdateInfo(final Pair<String, String> highlightValue,
		final Result source,
		final Result target,
		final DedupConfig dedupConfig) {
		return new UpdateInfo<>(
			Topic.ENRICH_MISSING_AUTHOR_ORCID,
			highlightValue, source, target,
			(p, pair) -> p.getCreators().add(pair.getLeft() + " - ORCID: " + pair.getRight()),
			pair -> pair.getLeft() + "::" + pair.getRight(),
			dedupConfig);
	}
}
