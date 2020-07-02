
package eu.dnetlib.dhp.broker.oa.matchers.relatedProjects;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.broker.objects.OaBrokerProject;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;

public class EnrichMoreProject extends UpdateMatcher<OaBrokerProject> {

	public EnrichMoreProject() {
		super(20,
			prj -> Topic.ENRICH_MORE_PROJECT,
			(p, prj) -> p.getProjects().add(prj),
			prj -> projectAsString(prj));
	}

	private static String projectAsString(final OaBrokerProject prj) {
		return prj.getFunder() + "::" + prj.getFundingProgram() + "::" + prj.getCode();
	}

	@Override
	protected List<OaBrokerProject> findDifferences(final OaBrokerMainEntity source,
		final OaBrokerMainEntity target) {

		if (target.getProjects().size() >= BrokerConstants.MAX_LIST_SIZE) {
			return new ArrayList<>();
		}

		final Set<String> existingProjects = target
			.getProjects()
			.stream()
			.map(EnrichMoreProject::projectAsString)
			.collect(Collectors.toSet());

		return source
			.getProjects()
			.stream()
			.filter(p -> !existingProjects.contains(projectAsString(p)))
			.collect(Collectors.toList());
	}

}
