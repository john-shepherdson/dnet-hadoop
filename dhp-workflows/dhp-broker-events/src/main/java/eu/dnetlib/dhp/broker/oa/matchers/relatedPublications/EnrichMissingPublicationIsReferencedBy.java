
package eu.dnetlib.dhp.broker.oa.matchers.relatedPublications;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.schema.common.ModelConstants;

public class EnrichMissingPublicationIsReferencedBy extends AbstractEnrichMissingPublication {

	public EnrichMissingPublicationIsReferencedBy() {
		super(Topic.ENRICH_MISSING_PUBLICATION_IS_REFERENCED_BY);
	}

	@Override
	protected boolean filterByType(final String relType) {
		return relType.equals(ModelConstants.IS_REFERENCED_BY);
	}
}
