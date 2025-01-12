
package eu.dnetlib.dhp.broker.oa.matchers.relatedProjects;

import java.util.ArrayList;
import java.util.List;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.broker.objects.OaBrokerProject;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;

public class EnrichMissingProject extends UpdateMatcher<OaBrokerProject> {

	public EnrichMissingProject() {
		super(20,
			prj -> Topic.ENRICH_MISSING_PROJECT,
			(p, prj) -> p.getProjects().add(prj),
			OaBrokerProject::getOpenaireId);
	}

	@Override
	protected List<OaBrokerProject> findDifferences(final OaBrokerMainEntity source, final OaBrokerMainEntity target) {
		if (target.getProjects().isEmpty()) {
			return source.getProjects();
		} else {
			return new ArrayList<>();
		}
	}
}
