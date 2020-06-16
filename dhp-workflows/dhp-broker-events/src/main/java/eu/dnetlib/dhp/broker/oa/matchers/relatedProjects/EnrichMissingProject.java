
package eu.dnetlib.dhp.broker.oa.matchers.relatedProjects;

import java.util.ArrayList;
import java.util.List;

import eu.dnetlib.broker.objects.OpenaireBrokerResult;
import eu.dnetlib.broker.objects.Project;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;

public class EnrichMissingProject extends UpdateMatcher<Project> {

	public EnrichMissingProject() {
		super(true,
			prj -> Topic.ENRICH_MISSING_PROJECT,
			(p, prj) -> p.getProjects().add(prj),
			prj -> prj.getFunder() + "::" + prj.getFundingProgram() + prj.getCode());
	}

	@Override
	protected List<Project> findDifferences(final OpenaireBrokerResult source, final OpenaireBrokerResult target) {
		if (target.getProjects().isEmpty()) {
			return source.getProjects();
		} else {
			return new ArrayList<>();
		}
	}
}
