
package eu.dnetlib.dhp.broker.oa.matchers.relatedProjects;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.Project;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.RelatedProject;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;

public class EnrichMissingProject
	extends UpdateMatcher<eu.dnetlib.broker.objects.Project> {

	public EnrichMissingProject() {
		super(true,
			prj -> Topic.ENRICH_MISSING_PROJECT,
			(p, prj) -> p.getProjects().add(prj),
			prj -> prj.getFunder() + "::" + prj.getFundingProgram() + prj.getCode());
	}

	@Override
	protected List<Project> findDifferences(final ResultWithRelations source, final ResultWithRelations target) {
		if (source.getProjects().isEmpty()) {
			return Arrays.asList();
		} else {
			return target
				.getProjects()
				.stream()
				.map(RelatedProject::getRelProject)
				.map(ConversionUtils::oafProjectToBrokerProject)
				.collect(Collectors.toList());
		}
	}
}
