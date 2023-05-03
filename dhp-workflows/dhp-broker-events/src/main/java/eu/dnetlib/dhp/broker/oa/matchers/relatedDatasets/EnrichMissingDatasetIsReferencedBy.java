
package eu.dnetlib.dhp.broker.oa.matchers.relatedDatasets;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class EnrichMissingDatasetIsReferencedBy extends AbstractEnrichMissingDataset {

	public EnrichMissingDatasetIsReferencedBy() {
		super(Topic.ENRICH_MISSING_DATASET_IS_REFERENCED_BY);
	}

	@Override
	protected boolean filterByType(final String relType) {
		
		return relType.equals(Relation.RELCLASS.IsReferencedBy);
	}

}
