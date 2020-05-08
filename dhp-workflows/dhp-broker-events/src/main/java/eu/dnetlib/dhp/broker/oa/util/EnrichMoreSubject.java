
package eu.dnetlib.dhp.broker.oa.util;

import java.util.Arrays;
import java.util.List;

import eu.dnetlib.broker.objects.OpenAireEventPayload;
import eu.dnetlib.dhp.schema.oaf.Result;

public class EnrichMoreSubject extends UpdateInfo<String> {

	public static List<EnrichMoreSubject> findUpdates(final Result source, final Result target) {
		// MESHEUROPMC
		// ARXIV
		// JEL
		// DDC
		// ACM

		return Arrays.asList();
	}

	private EnrichMoreSubject(final String subjectClassification, final String highlightValue, final float trust) {
		super("ENRICH/MORE/SUBJECT/" + subjectClassification, highlightValue, trust);
	}

	@Override
	public void compileHighlight(final OpenAireEventPayload payload) {
		payload.getHighlight().getSubjects().add(getHighlightValue());
	}

	@Override
	public String getHighlightValueAsString() {
		return getHighlightValue();
	}

}
