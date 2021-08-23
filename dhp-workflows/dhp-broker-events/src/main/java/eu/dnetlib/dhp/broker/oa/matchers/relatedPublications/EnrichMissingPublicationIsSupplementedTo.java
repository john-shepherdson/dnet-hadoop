
package eu.dnetlib.dhp.broker.oa.matchers.relatedPublications;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.schema.common.ModelConstants;

public class EnrichMissingPublicationIsSupplementedTo extends AbstractEnrichMissingPublication {

	public EnrichMissingPublicationIsSupplementedTo() {
		super(Topic.ENRICH_MISSING_PUBLICATION_IS_SUPPLEMENTED_TO);
	}

	@Override
	protected boolean filterByType(final String relType) {
		return relType.equals(ModelConstants.IS_SUPPLEMENT_TO);
	}

}
