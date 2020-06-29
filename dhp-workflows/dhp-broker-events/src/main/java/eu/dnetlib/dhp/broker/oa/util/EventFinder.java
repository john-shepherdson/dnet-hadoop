
package eu.dnetlib.dhp.broker.oa.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.util.LongAccumulator;

import eu.dnetlib.broker.objects.OaBrokerMainEntity;
import eu.dnetlib.dhp.broker.model.EventFactory;
import eu.dnetlib.dhp.broker.oa.matchers.UpdateMatcher;
import eu.dnetlib.dhp.broker.oa.matchers.relatedDatasets.EnrichMissingDatasetIsReferencedBy;
import eu.dnetlib.dhp.broker.oa.matchers.relatedDatasets.EnrichMissingDatasetIsRelatedTo;
import eu.dnetlib.dhp.broker.oa.matchers.relatedDatasets.EnrichMissingDatasetIsSupplementedBy;
import eu.dnetlib.dhp.broker.oa.matchers.relatedDatasets.EnrichMissingDatasetIsSupplementedTo;
import eu.dnetlib.dhp.broker.oa.matchers.relatedDatasets.EnrichMissingDatasetReferences;
import eu.dnetlib.dhp.broker.oa.matchers.relatedProjects.EnrichMissingProject;
import eu.dnetlib.dhp.broker.oa.matchers.relatedProjects.EnrichMoreProject;
import eu.dnetlib.dhp.broker.oa.matchers.relatedPublications.EnrichMissingPublicationIsReferencedBy;
import eu.dnetlib.dhp.broker.oa.matchers.relatedPublications.EnrichMissingPublicationIsRelatedTo;
import eu.dnetlib.dhp.broker.oa.matchers.relatedPublications.EnrichMissingPublicationIsSupplementedBy;
import eu.dnetlib.dhp.broker.oa.matchers.relatedPublications.EnrichMissingPublicationIsSupplementedTo;
import eu.dnetlib.dhp.broker.oa.matchers.relatedPublications.EnrichMissingPublicationReferences;
import eu.dnetlib.dhp.broker.oa.matchers.relatedSoftware.EnrichMissingSoftware;
import eu.dnetlib.dhp.broker.oa.matchers.relatedSoftware.EnrichMoreSoftware;
import eu.dnetlib.dhp.broker.oa.matchers.simple.EnrichMissingAbstract;
import eu.dnetlib.dhp.broker.oa.matchers.simple.EnrichMissingAuthorOrcid;
import eu.dnetlib.dhp.broker.oa.matchers.simple.EnrichMissingOpenAccess;
import eu.dnetlib.dhp.broker.oa.matchers.simple.EnrichMissingPid;
import eu.dnetlib.dhp.broker.oa.matchers.simple.EnrichMissingPublicationDate;
import eu.dnetlib.dhp.broker.oa.matchers.simple.EnrichMissingSubject;
import eu.dnetlib.dhp.broker.oa.matchers.simple.EnrichMoreOpenAccess;
import eu.dnetlib.dhp.broker.oa.matchers.simple.EnrichMorePid;
import eu.dnetlib.dhp.broker.oa.matchers.simple.EnrichMoreSubject;
import eu.dnetlib.dhp.broker.oa.util.aggregators.simple.ResultGroup;
import eu.dnetlib.pace.config.DedupConfig;

public class EventFinder {

	private static final List<UpdateMatcher<?>> matchers = new ArrayList<>();
	static {
		matchers.add(new EnrichMissingAbstract());
		matchers.add(new EnrichMissingAuthorOrcid());
		matchers.add(new EnrichMissingOpenAccess());
		matchers.add(new EnrichMissingPid());
		matchers.add(new EnrichMissingPublicationDate());
		matchers.add(new EnrichMissingSubject());
		matchers.add(new EnrichMoreOpenAccess());
		matchers.add(new EnrichMorePid());
		matchers.add(new EnrichMoreSubject());

		// Advanced matchers
		matchers.add(new EnrichMissingProject());
		matchers.add(new EnrichMoreProject());
		matchers.add(new EnrichMissingSoftware());
		matchers.add(new EnrichMoreSoftware());
		matchers.add(new EnrichMissingPublicationIsRelatedTo());
		matchers.add(new EnrichMissingPublicationIsReferencedBy());
		matchers.add(new EnrichMissingPublicationReferences());
		matchers.add(new EnrichMissingPublicationIsSupplementedTo());
		matchers.add(new EnrichMissingPublicationIsSupplementedBy());
		matchers.add(new EnrichMissingDatasetIsRelatedTo());
		matchers.add(new EnrichMissingDatasetIsReferencedBy());
		matchers.add(new EnrichMissingDatasetReferences());
		matchers.add(new EnrichMissingDatasetIsSupplementedTo());
		matchers.add(new EnrichMissingDatasetIsSupplementedBy());
		matchers.add(new EnrichMissingAbstract());
	}

	public static EventGroup generateEvents(final ResultGroup results,
		final DedupConfig dedupConfig,
		final Map<String, LongAccumulator> accumulators) {
		final List<UpdateInfo<?>> list = new ArrayList<>();

		for (final OaBrokerMainEntity target : results.getData()) {
			for (final UpdateMatcher<?> matcher : matchers) {
				list.addAll(matcher.searchUpdatesForRecord(target, results.getData(), dedupConfig, accumulators));
			}
		}

		return asEventGroup(list);
	}

	private static EventGroup asEventGroup(final List<UpdateInfo<?>> list) {
		final EventGroup events = new EventGroup();
		list.stream().map(EventFactory::newBrokerEvent).forEach(events::addElement);
		return events;
	}

	public static List<UpdateMatcher<?>> getMatchers() {
		return matchers;
	}

}
