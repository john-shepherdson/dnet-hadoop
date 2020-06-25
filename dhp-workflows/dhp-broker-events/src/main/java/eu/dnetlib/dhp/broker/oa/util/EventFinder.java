
package eu.dnetlib.dhp.broker.oa.util;

import java.util.ArrayList;
import java.util.List;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.dhp.broker.model.EventFactory;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.matchers.simple.EnrichMissingAbstract;
import eu.dnetlib.dhp.broker.oa.util.aggregators.simple.ResultGroup;
import eu.dnetlib.pace.config.DedupConfig;

public class EventFinder {

	private static List<UpdateMatcher<?>> matchers = new ArrayList<>();
	static {
		matchers.add(new EnrichMissingAbstract());
		// matchers.add(new EnrichMissingAuthorOrcid());
		// matchers.add(new EnrichMissingOpenAccess());
		// matchers.add(new EnrichMissingPid());
		// matchers.add(new EnrichMissingPublicationDate());
		// matchers.add(new EnrichMissingSubject());
		// matchers.add(new EnrichMoreOpenAccess());
		// matchers.add(new EnrichMorePid());
		// matchers.add(new EnrichMoreSubject());

		// // Advanced matchers
		// matchers.add(new EnrichMissingProject());
		// matchers.add(new EnrichMoreProject());
		// matchers.add(new EnrichMissingSoftware());
		// matchers.add(new EnrichMoreSoftware());
		// matchers.add(new EnrichMissingPublicationIsRelatedTo());
		// matchers.add(new EnrichMissingPublicationIsReferencedBy());
		// matchers.add(new EnrichMissingPublicationReferences());
		// matchers.add(new EnrichMissingPublicationIsSupplementedTo());
		// matchers.add(new EnrichMissingPublicationIsSupplementedBy());
		// matchers.add(new EnrichMissingDatasetIsRelatedTo());
		// matchers.add(new EnrichMissingDatasetIsReferencedBy());
		// matchers.add(new EnrichMissingDatasetReferences());
		// matchers.add(new EnrichMissingDatasetIsSupplementedTo());
		// matchers.add(new EnrichMissingDatasetIsSupplementedBy());
		// matchers.add(new EnrichMissingAbstract());
	}

	public static EventGroup generateEvents(final ResultGroup results, final DedupConfig dedupConfig) {
		final List<UpdateInfo<?>> list = new ArrayList<>();

		for (final OaBrokerMainEntity target : results.getData()) {
			for (final UpdateMatcher<?> matcher : matchers) {
				list.addAll(matcher.searchUpdatesForRecord(target, results.getData(), dedupConfig));
			}
		}

		return asEventGroup(list);
	}

	private static EventGroup asEventGroup(final List<UpdateInfo<?>> list) {
		final EventGroup events = new EventGroup();
		list.stream().map(EventFactory::newBrokerEvent).forEach(events::addElement);
		return events;
	}

}
