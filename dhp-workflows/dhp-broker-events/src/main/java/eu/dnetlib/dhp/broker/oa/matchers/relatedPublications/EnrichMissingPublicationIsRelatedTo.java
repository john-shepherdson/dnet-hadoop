
package eu.dnetlib.dhp.broker.oa.matchers.relatedPublications;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.schema.common.ModelConstants;

public class EnrichMissingPublicationIsRelatedTo extends AbstractEnrichMissingPublication {

	public EnrichMissingPublicationIsRelatedTo() {
		super(Topic.ENRICH_MISSING_PUBLICATION_IS_RELATED_TO);
	}

	@Override
	protected boolean filterByType(final String relType) {
		return relType.equals(ModelConstants.IS_RELATED_TO);
	}

}
