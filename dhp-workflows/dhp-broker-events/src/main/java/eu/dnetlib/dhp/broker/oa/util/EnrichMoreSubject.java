
package eu.dnetlib.dhp.broker.oa.util;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.schema.oaf.Result;

public class EnrichMoreSubject extends UpdateMatcher<Pair<String, String>> {

	public EnrichMoreSubject() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<Pair<String, String>>> findUpdates(final Result source, final Result target) {
		// MESHEUROPMC
		// ARXIV
		// JEL
		// DDC
		// ACM

		return Arrays.asList();
	}

	@Override
	public UpdateInfo<Pair<String, String>> generateUpdateInfo(final Pair<String, String> highlightValue,
		final Result source, final Result target) {

		return new UpdateInfo<>(
			Topic.fromPath("ENRICH/MORE/SUBJECT/" + highlightValue.getLeft()),
			highlightValue, source, target,
			(p, pair) -> p.getSubjects().add(pair.getRight()),
			pair -> pair.getLeft() + "::" + pair.getRight());
	}

}
