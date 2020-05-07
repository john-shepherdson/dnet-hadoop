package eu.dnetlib.dhp.broker.oa.util;

import java.util.Arrays;
import java.util.List;

import eu.dnetlib.broker.objects.OpenAireEventPayload;
import eu.dnetlib.dhp.schema.oaf.Result;

public class EnrichMissingAbstract extends UpdateInfo<String> {

	public static List<EnrichMissingAbstract> findUpdates(final Result source, final Result target) {
		// return Arrays.asList(new EnrichMissingAbstract("xxxxxxx", 0.9f));
		return Arrays.asList();
	}

	private EnrichMissingAbstract(final String highlightValue, final float trust) {
		super("ENRICH/MISSING/ABSTRACT", highlightValue, trust);
	}

	@Override
	public void compileHighlight(final OpenAireEventPayload payload) {
		payload.getHighlight().getAbstracts().add(getHighlightValue());
	}

	@Override
	public String getHighlightValueAsString() {
		return getHighlightValue();
	}

}
