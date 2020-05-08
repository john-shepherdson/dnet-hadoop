
package eu.dnetlib.dhp.broker.oa.util;

import java.util.Arrays;
import java.util.List;

import eu.dnetlib.broker.objects.OpenAireEventPayload;
import eu.dnetlib.dhp.schema.oaf.Result;

public class EnrichMissingAuthorOrcid extends UpdateInfo<String> {

	public static List<EnrichMissingAuthorOrcid> findUpdates(final Result source, final Result target) {
		// return Arrays.asList(new EnrichMissingAbstract("xxxxxxx", 0.9f));
		return Arrays.asList();
	}

	private EnrichMissingAuthorOrcid(final String highlightValue, final float trust) {
		super("ENRICH/MISSING/AUTHOR/ORCID", highlightValue, trust);
	}

	@Override
	public void compileHighlight(final OpenAireEventPayload payload) {
		// TODO
	}

	@Override
	public String getHighlightValueAsString() {
		return getHighlightValue();
	}

}
