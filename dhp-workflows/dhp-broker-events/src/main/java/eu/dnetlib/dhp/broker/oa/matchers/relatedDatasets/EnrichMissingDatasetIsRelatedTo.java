
package eu.dnetlib.dhp.broker.oa.matchers.relatedDatasets;

import eu.dnetlib.dhp.broker.model.Topic;
import eu.dnetlib.dhp.schema.oaf.Relation;

public class EnrichMissingDatasetIsRelatedTo extends AbstractEnrichMissingDataset {

	public EnrichMissingDatasetIsRelatedTo() {
		super(Topic.ENRICH_MISSING_DATASET_IS_RELATED_TO);
	}

	@Override
	protected boolean filterByType(final String relType) {
		
		return relType.equals(Relation.RELCLASS.IsRelatedTo);
	}

}
