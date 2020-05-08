
package eu.dnetlib.dhp.broker.oa.util;

import java.util.Arrays;
import java.util.List;

import eu.dnetlib.broker.objects.Instance;
import eu.dnetlib.broker.objects.OpenAireEventPayload;
import eu.dnetlib.dhp.schema.oaf.Result;

public class EnrichMoreOpenAccess extends UpdateInfo<Instance> {

	public static List<EnrichMoreOpenAccess> findUpdates(final Result source, final Result target) {
		// return Arrays.asList(new EnrichMissingAbstract("xxxxxxx", 0.9f));
		return Arrays.asList();
	}

	private EnrichMoreOpenAccess(final Instance highlightValue, final float trust) {
		super("ENRICH/MORE/OPENACCESS_VERSION", highlightValue, trust);
	}

	@Override
	public void compileHighlight(final OpenAireEventPayload payload) {
		payload.getHighlight().getInstances().add(getHighlightValue());
	}

	@Override
	public String getHighlightValueAsString() {
		return getHighlightValue().getUrl();
	}

}
