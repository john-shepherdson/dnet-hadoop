
package eu.dnetlib.dhp.broker.oa.util;

import java.util.Arrays;
import java.util.List;

import eu.dnetlib.broker.objects.Project;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.schema.oaf.Result;

public class EnrichMissingProject extends UpdateMatcher<Project> {

	public EnrichMissingProject() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<Project>> findUpdates(final Result source, final Result target) {
		// return Arrays.asList(new EnrichMissingAbstract("xxxxxxx", 0.9f));
		return Arrays.asList();
	}

	@Override
	public UpdateInfo<Project> generateUpdateInfo(final Project highlightValue, final Result source,
		final Result target) {
		return new UpdateInfo<>(
			Topic.ENRICH_MISSING_PROJECT,
			highlightValue, source, target,
			(p, prj) -> p.getProjects().add(prj),
			prj -> prj.getFunder() + "::" + prj.getFundingProgram() + prj.getCode());
	}

}
