
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.aggregators.withRels.ResultWithRelations;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class EnrichMissingAuthorOrcid extends UpdateMatcher<String> {

	public EnrichMissingAuthorOrcid() {
		super(true,
			aut -> Topic.ENRICH_MISSING_AUTHOR_ORCID,
			(p, aut) -> p.getCreators().add(aut),
			aut -> aut);
	}

	@Override
	protected List<String> findDifferences(final ResultWithRelations source,
		final ResultWithRelations target) {

		final Set<String> existingOrcids = target
			.getResult()
			.getAuthor()
			.stream()
			.map(Author::getPid)
			.flatMap(List::stream)
			.filter(pid -> pid.getQualifier().getClassid().equalsIgnoreCase("orcid"))
			.map(pid -> pid.getValue())
			.collect(Collectors.toSet());

		final List<String> list = new ArrayList<>();

		for (final Author author : source.getResult().getAuthor()) {
			final String name = author.getFullname();

			for (final StructuredProperty pid : author.getPid()) {
				if (pid.getQualifier().getClassid().equalsIgnoreCase("orcid")
					&& !existingOrcids.contains(pid.getValue())) {
					list.add(name + " [ORCID: " + pid.getValue() + "]");
				}
			}
		}

		return list;
	}
}
