
package eu.dnetlib.dhp.broker.oa.matchers.relatedPublications;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class EnrichMissingPublicationReferences extends AbstractEnrichMissingPublication {

	public EnrichMissingPublicationReferences() {
		super(Topic.ENRICH_MISSING_PUBLICATION_REFERENCES);
	}

	@Override
	protected boolean filterByType(final String relType) {
		return relType.equals(Relation.RELCLASS.References);
	}

}
