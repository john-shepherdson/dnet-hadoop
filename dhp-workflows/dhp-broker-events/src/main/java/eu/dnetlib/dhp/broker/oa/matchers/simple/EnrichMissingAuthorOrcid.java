
package eu.dnetlib.dhp.broker.oa.matchers.simple;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import eu.dnetlib.broker.objects.OaBrokerAuthor;
import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.util.BrokerConstants;

public class EnrichMissingAuthorOrcid extends UpdateMatcher<OaBrokerAuthor> {

	public EnrichMissingAuthorOrcid() {
		super(40,
			aut -> Topic.ENRICH_MISSING_AUTHOR_ORCID,
			(p, aut) -> p.getCreators().add(aut),
			aut -> aut.getOrcid());
	}

	@Override
	protected List<OaBrokerAuthor> findDifferences(final OaBrokerMainEntity source,
		final OaBrokerMainEntity target) {

		if (target.getCreators().size() >= BrokerConstants.MAX_LIST_SIZE) {
			return new ArrayList<>();
		}

		final Set<String> existingOrcids = target
			.getCreators()
			.stream()
			.map(OaBrokerAuthor::getOrcid)
			.filter(StringUtils::isNotBlank)
			.collect(Collectors.toSet());

		return source
			.getCreators()
			.stream()
			.filter(a -> StringUtils.isNotBlank(a.getOrcid()))
			.filter(a -> !existingOrcids.contains(a.getOrcid()))
			.collect(Collectors.toList());

	}
}
