
package eu.dnetlib.dhp.broker.oa.matchers.relatedProjects;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.ConversionUtils;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.pace.config.DedupConfig;

public class EnrichMoreProject extends UpdateMatcher<Pair<Result, List<Project>>, eu.dnetlib.broker.objects.Project> {

	public EnrichMoreProject() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<eu.dnetlib.broker.objects.Project>> findUpdates(final Pair<Result, List<Project>> source,
		final Pair<Result, List<Project>> target,
		final DedupConfig dedupConfig) {

		final Set<String> existingProjects = source
			.getRight()
			.stream()
			.map(Project::getId)
			.collect(Collectors.toSet());

		return target
			.getRight()
			.stream()
			.filter(p -> !existingProjects.contains(p.getId()))
			.map(ConversionUtils::oafProjectToBrokerProject)
			.map(p -> generateUpdateInfo(p, source, target, dedupConfig))
			.collect(Collectors.toList());
	}

	public UpdateInfo<eu.dnetlib.broker.objects.Project> generateUpdateInfo(
		final eu.dnetlib.broker.objects.Project highlightValue,
		final Pair<Result, List<Project>> source,
		final Pair<Result, List<Project>> target,
		final DedupConfig dedupConfig) {
		return new UpdateInfo<>(
			Topic.ENRICH_MORE_PROJECT,
			highlightValue, source.getLeft(), target.getLeft(),
			(p, prj) -> p.getProjects().add(prj),
			prj -> prj.getFunder() + "::" + prj.getFundingProgram() + prj.getCode(), dedupConfig);
	}

}
