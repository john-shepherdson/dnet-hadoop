
package eu.dnetlib.dhp.broker.oa.matchers.relatedProjects;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedProject;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;
import eu.dnetlib.pace.config.DedupConfig;

public class EnrichMissingProject
	extends UpdateMatcher<eu.dnetlib.broker.objects.Project> {

	public EnrichMissingProject() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<eu.dnetlib.broker.objects.Project>> findUpdates(final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {

		if (source.getProjects().isEmpty()) {
			return Arrays.asList();
		} else {
			return target
				.getProjects()
				.stream()
				.map(RelatedProject::getRelProject)
				.map(ConversionUtils::oafProjectToBrokerProject)
				.map(p -> generateUpdateInfo(p, source, target, dedupConfig))
				.collect(Collectors.toList());
		}
	}

	public UpdateInfo<eu.dnetlib.broker.objects.Project> generateUpdateInfo(
		final eu.dnetlib.broker.objects.Project highlightValue,
		final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {
		return new UpdateInfo<>(
			Topic.ENRICH_MISSING_PROJECT,
			highlightValue, source, target,
			(p, prj) -> p.getProjects().add(prj),
			prj -> prj.getFunder() + "::" + prj.getFundingProgram() + prj.getCode(), dedupConfig);
	}

}
