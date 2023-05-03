
package eu.dnetlib.dhp.broker.oa.matchers.relatedPublications;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class EnrichMissingPublicationIsSupplementedBy extends AbstractEnrichMissingPublication {

	public EnrichMissingPublicationIsSupplementedBy() {
		super(Topic.ENRICH_MISSING_PUBLICATION_IS_SUPPLEMENTED_BY);
	}

	@Override
	protected boolean filterByType(final String relType) {
		return relType.equals(Relation.RELCLASS.IsSupplementedBy);
	}
}
