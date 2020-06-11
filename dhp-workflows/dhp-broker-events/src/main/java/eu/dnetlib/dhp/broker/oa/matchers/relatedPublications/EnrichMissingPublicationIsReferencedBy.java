
package eu.dnetlib.dhp.broker.oa.matchers.relatedPublications;

import eu.dnetlib.dhp.broker.model.Topic;

public class EnrichMissingPublicationIsReferencedBy extends AbstractEnrichMissingPublication {

	public EnrichMissingPublicationIsReferencedBy() {
		super(Topic.ENRICH_MISSING_PUBLICATION_IS_REFERENCED_BY);
	}

	@Override
	protected boolean filterByType(final String relType) {
		return relType.equals("isReferencedBy");
	}
}
