
package eu.dnetlib.dhp.broker.oa.matchers.relatedProjects;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Result;

public class EnrichMissingProject
	extends UpdateMatcher<Pair<Result, List<Project>>, eu.dnetlib.broker.objects.Project> {

	public EnrichMissingProject() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<eu.dnetlib.broker.objects.Project>> findUpdates(final Pair<Result, List<Project>> source,
		final Pair<Result, List<Project>> target) {
		// TODO
		return Arrays.asList();
	}

	@Override
	public UpdateInfo<eu.dnetlib.broker.objects.Project> generateUpdateInfo(
		final eu.dnetlib.broker.objects.Project highlightValue,
		final Pair<Result, List<Project>> source,
		final Pair<Result, List<Project>> target) {
		return new UpdateInfo<>(
			Topic.ENRICH_MISSING_PROJECT,
			highlightValue, source.getLeft(), target.getLeft(),
			(p, prj) -> p.getProjects().add(prj),
			prj -> prj.getFunder() + "::" + prj.getFundingProgram() + prj.getCode());
	}

}
