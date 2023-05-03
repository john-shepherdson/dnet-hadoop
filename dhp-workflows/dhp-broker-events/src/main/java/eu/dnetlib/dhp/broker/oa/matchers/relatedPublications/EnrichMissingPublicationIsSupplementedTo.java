
package eu.dnetlib.dhp.broker.oa.matchers.relatedPublications;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class EnrichMissingPublicationIsSupplementedTo extends AbstractEnrichMissingPublication {

	public EnrichMissingPublicationIsSupplementedTo() {
		super(Topic.ENRICH_MISSING_PUBLICATION_IS_SUPPLEMENTED_TO);
	}

	@Override
	protected boolean filterByType(final String relType) {
		return relType.equals(Relation.RELCLASS.IsSupplementTo);
	}

}
