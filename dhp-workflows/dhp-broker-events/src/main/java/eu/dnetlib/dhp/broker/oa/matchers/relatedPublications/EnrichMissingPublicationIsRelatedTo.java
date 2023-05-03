
package eu.dnetlib.dhp.broker.oa.matchers.relatedPublications;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class EnrichMissingPublicationIsRelatedTo extends AbstractEnrichMissingPublication {

	public EnrichMissingPublicationIsRelatedTo() {
		super(Topic.ENRICH_MISSING_PUBLICATION_IS_RELATED_TO);
	}

	@Override
	protected boolean filterByType(final String relType) {
		return relType.equals(Relation.RELCLASS.IsRelatedTo);
	}

}
