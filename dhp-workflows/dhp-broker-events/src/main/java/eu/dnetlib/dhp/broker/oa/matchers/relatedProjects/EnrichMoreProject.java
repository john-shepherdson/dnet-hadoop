
package eu.dnetlib.dhp.broker.oa.matchers.relatedProjects;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.broker.objects.OpenaireBrokerResult;
import eu.dnetlib.broker.objects.Project;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;

public class EnrichMoreProject extends UpdateMatcher<Project> {

	public EnrichMoreProject() {
		super(true,
			prj -> Topic.ENRICH_MORE_PROJECT,
			(p, prj) -> p.getProjects().add(prj),
			prj -> projectAsString(prj));
	}

	private static String projectAsString(final Project prj) {
		return prj.getFunder() + "::" + prj.getFundingProgram() + "::" + prj.getCode();
	}

	@Override
	protected List<eu.dnetlib.broker.objects.Project> findDifferences(final OpenaireBrokerResult source,
		final OpenaireBrokerResult target) {

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
