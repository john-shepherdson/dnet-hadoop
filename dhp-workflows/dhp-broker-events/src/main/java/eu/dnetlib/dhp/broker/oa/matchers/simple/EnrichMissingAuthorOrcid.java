
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.UpdateInfo;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.pace.config.DedupConfig;

public class EnrichMissingAuthorOrcid extends UpdateMatcher<String> {

	public EnrichMissingAuthorOrcid() {
		super(true);
	}

	@Override
	protected List<UpdateInfo<String>> findUpdates(final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {

		final Set<String> existingOrcids = target
			.getResult()
			.getAuthor()
			.stream()
			.map(Author::getPid)
			.flatMap(List::stream)
			.filter(pid -> pid.getQualifier().getClassid().equalsIgnoreCase("orcid"))
			.map(pid -> pid.getValue())
			.collect(Collectors.toSet());

		final List<UpdateInfo<String>> list = new ArrayList<>();

		for (final Author author : source.getResult().getAuthor()) {
			final String name = author.getFullname();

			for (final StructuredProperty pid : author.getPid()) {
				if (pid.getQualifier().getClassid().equalsIgnoreCase("orcid")
					&& !existingOrcids.contains(pid.getValue())) {
					list
						.add(
							generateUpdateInfo(name + " [ORCID: " + pid.getValue() + "]", source, target, dedupConfig));
					;
				}
			}
		}

		return list;
	}

	public UpdateInfo<String> generateUpdateInfo(final String highlightValue,
		final ResultWithRelations source,
		final ResultWithRelations target,
		final DedupConfig dedupConfig) {
		return new UpdateInfo<>(
			Topic.ENRICH_MISSING_AUTHOR_ORCID,
			highlightValue, source, target,
			(p, aut) -> p.getCreators().add(aut),
			aut -> aut,
			dedupConfig);
	}
}
