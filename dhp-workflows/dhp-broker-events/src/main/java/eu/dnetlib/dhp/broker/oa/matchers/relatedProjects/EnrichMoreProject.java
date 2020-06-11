
package eu.dnetlib.dhp.broker.oa.matchers.relatedProjects;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedProject;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.pace.config.DedupConfig;

public class EnrichMoreProject extends UpdateMatcher<eu.dnetlib.broker.objects.Project> {

	public EnrichMoreProject() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<eu.dnetlib.broker.objects.Project>> findUpdates(final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {

		final Set<String> existingProjects = source
			.getProjects()
			.stream()
			.map(RelatedProject::getRelProject)
			.map(Project::getId)
			.collect(Collectors.toSet());

		return target
			.getProjects()
			.stream()
			.map(RelatedProject::getRelProject)
			.filter(p -> !existingProjects.contains(p.getId()))
			.map(ConversionUtils::oafProjectToBrokerProject)
			.map(p -> generateUpdateInfo(p, source, target, dedupConfig))
			.collect(Collectors.toList());
	}

	public UpdateInfo<eu.dnetlib.broker.objects.Project> generateUpdateInfo(
		final eu.dnetlib.broker.objects.Project highlightValue,
		final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {
		return new UpdateInfo<>(
			Topic.ENRICH_MORE_PROJECT,
			highlightValue, source, target,
			(p, prj) -> p.getProjects().add(prj),
			prj -> prj.getFunder() + "::" + prj.getFundingProgram() + prj.getCode(), dedupConfig);
	}

}
