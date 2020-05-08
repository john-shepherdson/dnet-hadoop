
package eu.dnetlib.dhp.broker.oa.util;

import java.util.Arrays;
import java.util.List;

import eu.dnetlib.broker.objects.OpenAireEventPayload;
import eu.dnetlib.broker.objects.Project;
import eu.dnetlib.dhp.schema.oaf.Result;

public class EnrichMissingProject extends UpdateInfo<Project> {

	public static List<EnrichMissingProject> findUpdates(final Result source, final Result target) {
		// return Arrays.asList(new EnrichMissingAbstract("xxxxxxx", 0.9f));
		return Arrays.asList();
	}

	private EnrichMissingProject(final Project highlightValue, final float trust) {
		super("ENRICH/MISSING/PROJECT", highlightValue, trust);
	}

	@Override
	public void compileHighlight(final OpenAireEventPayload payload) {
		payload.getHighlight().getProjects().add(getHighlightValue());
	}

	@Override
	public String getHighlightValueAsString() {
		return getHighlightValue().getFunder() + "::" + getHighlightValue().getFundingProgram()
			+ getHighlightValue().getCode();
	}

}
