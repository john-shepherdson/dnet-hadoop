
package eu.dnetlib.dhp.broker.oa.matchers.relatedProjects;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedProject;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;
import eu.dnetlib.dhp.schema.oaf.Project;

public class EnrichMoreProject extends UpdateMatcher<eu.dnetlib.broker.objects.Project> {

	public EnrichMoreProject() {
		super(true,
			prj -> Topic.ENRICH_MORE_PROJECT,
			(p, prj) -> p.getProjects().add(prj),
			prj -> prj.getFunder() + "::" + prj.getFundingProgram() + prj.getCode());
	}

	@Override
	protected List<eu.dnetlib.broker.objects.Project> findDifferences(final ResultWithRelations source,
		final ResultWithRelations target) {

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
			.collect(Collectors.toList());
	}

}
